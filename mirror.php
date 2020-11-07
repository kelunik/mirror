#!/usr/bin/env php
<?php

use Amp\Loop;
use Composer\Mirror\Mirror;
use Symfony\Component\Lock\LockFactory;
use Symfony\Component\Lock\Store\FlockStore;

ini_set('memory_limit', '3G');
ini_set('display_errors', true);
ini_set('error_reporting', -1);
ini_set('date.timezone', 'UTC');

// define('AMP_WORKER', true);

if (!extension_loaded('zlib')) {
    echo 'This requires the zlib/zip extension to be loaded';
    exit(1);
}

require __DIR__ . '/vendor/autoload.php';

set_error_handler(static function ($errno, $errstr, $file, $line) {
    if (error_reporting() & $errno) {
        throw new ErrorException($errstr, $errno, E_ERROR, $file, $line);
    }

    return false;
});

Loop::delay(30000, function () {
    define('EXIT', true);
});

// TODO Reenable gzip check
// if ($rootResp->getHeader('content-encoding') !== 'gzip') {
//     throw new \Exception('Expected gzip encoded responses, something is off');
// }

$lockName = 'mirror';
$config = require __DIR__ . '/mirror.config.php';
$isGC = false;
$isV2 = false;
$isV1 = false;
$isResync = false;

if (in_array('--gc', $_SERVER['argv'], true)) {
    $lockName .= '-gc';
    $isGC = true;
} elseif (in_array('--v2', $_SERVER['argv'], true)) {
    $lockName .= '-v2';
    $isV2 = true;
} elseif (in_array('--v1', $_SERVER['argv'], true)) {
    $isV1 = true;
    // default mode
} elseif (in_array('--resync', $_SERVER['argv'], true)) {
    // resync uses same lock name as --v2 to make sure they can not run in parallel
    $lockName .= '-v2';
    $isResync = true;
} else {
    throw new RuntimeException('Missing one of --gc, --v1 or --v2 modes');
}

$lockFactory = new LockFactory(new FlockStore(sys_get_temp_dir()));
$lock = $lockFactory->createLock($lockName, 3600);

// if resync is running, we wait for the lock to be
// acquired in case a v2 process is still running
// otherwise abort immediately
if (!$lock->acquire($isResync)) {
    // sleep so supervisor assumes a correct start and we avoid restarting too quickly, then exit
    sleep(3);
    exit(0);
}

$mirror = new Mirror($config);

try {
    if ($isGC) {
        $mirror->gc();
        $lock->release();
        exit(0);
    }

    if ($isResync) {
        $mirror->resync($mirror->getV2Timestamp());
        $lock->release();
        exit(0);
    }

    $iterations = $config['iterations'];
    $hasSyncedRoot = false;

    while ($iterations--) {
        if ($isV2) {
            // sync root only once in a while as on a v2 only repo it rarely changes
            if (($iterations % 20) === 0 || $hasSyncedRoot === false) {
                $mirror->syncRootOnV2();
                $hasSyncedRoot = true;
            }

            $mirror->syncV2();
        } elseif ($isV1) {
            $mirror->sync();
        }

        sleep($config['iteration_interval']);
        $lock->refresh();
    }
} catch (Throwable $e) {
    // sleep so supervisor assumes a correct start and we avoid restarting too quickly, then rethrow
    $mirror->statsdIncrement('mirror.hard_failure');

    sleep(3);

    echo 'Mirror ' . ($isV2 ? 'v2' : '') . ' job failed at ' . date('Y-m-d H:i:s') . PHP_EOL;
    echo '[' . get_class($e) . '] ' . $e->getMessage() . PHP_EOL;

    throw $e;
} finally {
    $lock->release();
}

exit(0);
