<?php

namespace Composer\Mirror;

use Amp\AsyncGenerator;
use Amp\File;
use Amp\Http\Client\HttpClient;
use Amp\Http\Client\HttpClientBuilder;
use Amp\Http\Client\Interceptor\SetRequestHeader;
use Amp\Http\Client\Request;
use Amp\Http\Client\Response;
use Amp\Http\Message as HttpMessage;
use Amp\Loop;
use Amp\Pipeline;
use Amp\Socket\Socket;
use Amp\Sync\LocalSemaphore;
use Symfony\Component\Finder\Finder;
use function Amp\async;
use function Amp\await;
use function Amp\defer;
use function Amp\delay;
use function Amp\Http\formatDateHeader;
use function Amp\Parallel\Worker\enqueue;
use function Amp\Parallel\Worker\enqueueCallable;
use function Amp\Socket\connect;

class Mirror
{
    private string $target;
    private string $userAgent;
    private string $url;
    private string $apiUrl;
    private string $hostname;

    private ?Socket $statsdSocket = null;

    private HttpClient $client;
    private RequestCounter $requestCounter;

    private bool $verbose;
    private bool $gzipOnly;
    private bool $syncRootOnV2;

    private string $outputBuffer = '';

    public function __construct(array $config)
    {
        $this->target = $config['target_dir'];
        $this->url = $config['repo_url'] ?? 'repo.packagist.org';
        $this->apiUrl = $config['api_url'] ?? 'packagist.org';
        $this->hostname = (string) ($config['repo_hostname'] ?? parse_url($this->url, PHP_URL_HOST));
        $this->userAgent = $config['user_agent'];
        $this->syncRootOnV2 = !($config['has_v1_mirror'] ?? true);
        $this->gzipOnly = $config['gzip_only'] ?? false;

        if (isset($config['statsd']) && is_array($config['statsd'])) {
            $this->statsdSocket = connect('udp://' . $config['statsd'][0] . ':' . $config['statsd'][1]);
        }

        $this->verbose = in_array('-v', $_SERVER['argv'], true);
        if ($this->verbose) {
            ini_set('display_errors', 1);
        }

        $this->initClient();

        Loop::repeat(1000, function () {
            static $last = 0;

            $count = $this->requestCounter->getRequestCount();
            $rps = $count - $last;
            $last = $count;

            async(fn () => File\write('rps', $rps . PHP_EOL));
        });

        Loop::repeat(500, function () {
            $output = $this->outputBuffer;
            $this->outputBuffer = '';

            echo $output;
        });
    }

    public function syncRootOnV2(): void
    {
        if (!$this->syncRootOnV2) {
            return;
        }

        $rootResponse = $this->download('/packages.json');
        $rootData = $rootResponse->getBody()->buffer();

        if (hash('sha256', $rootData) === $this->getHash('/packages.json')) {
            return;
        }

        $this->write('/packages.json', $rootData, 8, $this->getLastModified($rootResponse));
        $this->output('X');

        $this->statsdIncrement('mirror.sync_root');
    }

    public function getV2Timestamp(): int
    {
        $response = $this->client->request(new Request($this->apiUrl . '/metadata/changes.json'));
        $responseBody = $response->getBody()->buffer();

        $content = json_decode($responseBody, true, 512, JSON_THROW_ON_ERROR);
        if ($content !== null && $response->getStatus() === 400) {
            return $content['timestamp'];
        }

        throw new \RuntimeException('Failed to fetch timestamp from API, got invalid response ' . $response->getStatus() . ': ' . $responseBody);
    }

    public function syncV2(): void
    {
        $this->statsdIncrement('mirror.run');

        $timestampStore = './last_metadata_timestamp';
        if (!File\exists($timestampStore)) {
            $this->resync($this->getV2Timestamp());
            return;
        }

        $lastTime = trim(File\read($timestampStore));

        $changesResponse = $this->client->request(new Request($this->apiUrl . '/metadata/changes.json?since=' . $lastTime));
        $changesBody = $changesResponse->getBody()->buffer();
        $changes = json_decode($changesBody, true, 512, JSON_THROW_ON_ERROR);

        if ([] === $changes['actions']) {
            $this->output('No work' . PHP_EOL);
            File\write($timestampStore, $changes['timestamp']);
            return;
        }

        if ($changes['actions'][0]['type'] === 'resync') {
            $this->resync($changes['timestamp']);
            return;
        }

        $requests = new AsyncGenerator(function () use ($changes) {
            foreach ($changes['actions'] as $action) {
                if ($action['type'] === 'update') {
                    // package here can be foo/bar or foo/bar~dev, not strictly a package name
                    $providerPathV2 = '/p2/' . $action['package'] . '.json';
                    $userData = ['path' => $providerPathV2, 'minimumFilemtime' => $action['time']];

                    $request = new Request($this->url . $providerPathV2);
                    $request->setAttribute('user_data', $userData);

                    if (File\exists($this->target . $providerPathV2 . '.gz')) {
                        $request->setHeader('if-modified-since',
                            formatDateHeader(File\getModificationTime($this->target . $providerPathV2 . '.gz')));
                    }

                    // TODO Backpressure
                    yield $request;
                } elseif ($action['type'] === 'delete') {
                    $this->delete($action['package']);
                }
            }
        });

        $this->downloadV2Files($requests);

        $this->output(PHP_EOL);
        $this->output('Downloaded ' . $this->requestCounter->getRequestCount() . ' files' . PHP_EOL);
        File\write($timestampStore, $changes['timestamp']);

        // Delay to flush output
        delay(1000);
    }

    public function resync(int $timestamp): void
    {
        $this->output('Resync requested' . PHP_EOL);

        $listingResponse = $this->client->request(new Request($this->apiUrl . '/packages/list.json?' . md5(random_bytes(16))));
        $listingBody = $listingResponse->getBody()->buffer();

        $list = json_decode($listingBody, true, 512, JSON_THROW_ON_ERROR);

        // clean up existing files in case we still have outdated packages
        if (is_dir($this->target . '/p2')) {
            $finder = Finder::create()->directories()->ignoreVCS(true)->in($this->target . '/p2');
            $names = array_flip($list['packageNames']);

            foreach ($finder as $vendorDir) {
                foreach (glob(((string) $vendorDir) . '/*.json.gz') as $file) {
                    if (!preg_match('{/([^/]+/[^/]+?)(~dev)?\.json.gz$}', str_replace('\\', '/', $file), $match)) {
                        throw new \LogicException('Could not match package name from ' . $file);
                    }

                    if (!isset($names[$match[1]])) {
                        File\deleteFile((string) $file);
                        // also remove the version without .gz suffix if it exists
                        if (File\exists(substr((string) $file, 0, -3))) {
                            File\deleteFile(substr((string) $file, 0, -3));
                        }
                    }
                }
            }
        }

        // download all package data
        $requests = new AsyncGenerator(function () use ($list) {
            foreach ($list['packageNames'] as $package) {
                $providerPaths = [
                    '/p2/' . $package . '.json',
                    '/p2/' . $package . '~dev.json',
                ];

                foreach ($providerPaths as $path) {
                    $request = new Request($this->url . $path);
                    $request->setAttribute('user_data', ['path' => $path, 'minimumFilemtime' => 0]);

                    if (File\exists($this->target . $path . '.gz')) {
                        $request->setHeader('if-modified-since',
                            formatDateHeader(File\getModificationTime($this->target . $path . '.gz')));
                    }

                    yield $request;
                }
            }
        });

        $this->downloadV2Files($requests);

        $this->output(PHP_EOL);
        $this->output('Downloaded ' . $this->requestCounter->getRequestCount() . ' files' . PHP_EOL);

        File\write('./last_metadata_timestamp', $timestamp);

        $this->statsdIncrement('mirror.resync');

        // Delay to flush output
        delay(1000);
    }

    public function sync(): void
    {
        $this->statsdIncrement('mirror.run');

        $rootResponse = $this->download('/packages.json');
        $rootData = $rootResponse->getBody()->buffer();

        $hash = hash('sha256', $rootData);
        if ($hash === $this->getHash('/packages.json')) {
            $this->output('No work' . PHP_EOL);
        }

        $rootJson = json_decode($rootData, true, 512, JSON_THROW_ON_ERROR);
        if (null === $rootJson) {
            throw new \RuntimeException('Invalid JSON received for file /packages.json: ' . $rootData);
        }

        $listingsToWrite = [];

        $requests = new AsyncGenerator(function () use ($rootJson, &$listingsToWrite) {
            foreach ($rootJson['provider-includes'] as $listing => $listingOptions) {
                $listing = str_replace('%hash%', $listingOptions['sha256'], $listing);
                if (File\exists($this->target . '/' . $listing . '.gz')) {
                    continue;
                }

                $listingResponse = $this->download('/' . $listing);
                $listingData = $listingResponse->getBody()->buffer();

                if (hash('sha256', $listingData) !== $listingOptions['sha256']) {
                    throw new \RuntimeException('Invalid hash received for file /' . $listing);
                }

                $listingJson = json_decode($listingData, true, 512, JSON_THROW_ON_ERROR);
                if (null === $listingJson) {
                    throw new \RuntimeException('Invalid JSON received for file /' . $listing . ': ' . $listingData);
                }

                foreach ($listingJson['providers'] as $package => $packageOptions) {
                    $providerPath = '/p/' . $package . '$' . $packageOptions['sha256'] . '.json';
                    $providerPathAlternative = '/p/' . $package . '.json';

                    if (File\exists($this->target . $providerPath . '.gz')) {
                        continue;
                    }

                    $userData = [$providerPath, $providerPathAlternative, $packageOptions['sha256']];

                    $request = new Request($this->url . $providerPath);
                    $request->setAttribute('user_data', $userData);

                    yield $request;
                }

                $listingsToWrite['/' . $listing] = [
                    $listingData,
                    $this->getLastModified($listingResponse),
                ];
            }
        });

        $concurrency = 128;
        $semaphore = new LocalSemaphore($concurrency);

        while ($request = $requests->continue()) {
            if (defined('EXIT')) {
                break; // TODO Remove
            }

            $lock = $semaphore->acquire();

            defer(function () use ($request, $lock) {
                try {
                    $response = $this->client->request($request);

                    $status = $response->getStatus();
                    if ($status === 304) {
                        $this->output('-');
                        $this->statsdIncrement('mirror.not_modified');
                        return;
                    }

                    $userData = $response->getOriginalRequest()->getAttribute('user_data');

                    // provider v1
                    $providerData = $response->getBody()->buffer();
                    if (null === json_decode($providerData, true, 512, JSON_THROW_ON_ERROR)) {
                        throw new \RuntimeException('Invalid JSON received for file ' . $userData[0]);
                    }
                    if (hash('sha256', $providerData) !== $userData[2]) {
                        throw new \RuntimeException('Invalid hash received for file ' . $userData[0]);
                    }

                    $mtime = $this->getLastModified($response);

                    await([
                        async(fn() => $this->write($userData[0], $providerData, 7, $mtime)),
                        async(fn() => $this->write($userData[1], $providerData, 7, $mtime)),
                    ]);

                    $this->output('P');
                    $this->statsdIncrement('mirror.sync_provider');
                } catch (\Throwable $e) {
                    $this->statsdIncrement('mirror.provider_failure');

                    throw $e;
                } finally {
                    $lock->release();
                }
            });
        }

        // Acquire all locks to ensure all requests finished
        $locks = [];
        for ($j = 0; $j < $concurrency; $j++) {
            $locks[] = $semaphore->acquire();
        }

        foreach ($listingsToWrite as $listing => $listingData) {
            $this->write($listing, $listingData[0], 8, $listingData[1]);
            $this->output('L');
            $this->statsdIncrement('mirror.sync_listing');
        }

        $this->write('/packages.json', $rootData, 8, $this->getLastModified($rootResponse));
        $this->output('X');
        $this->statsdIncrement('mirror.sync_root');

        $this->output(PHP_EOL);
        $this->output('Downloaded ' . $this->requestCounter->getRequestCount() . ' files' . PHP_EOL);
    }

    public function gc(): void
    {
        // build up array of safe files
        $safeFiles = [];

        $rootFile = $this->target . '/packages.json.gz';
        if (!File\exists($rootFile)) {
            return;
        }

        $rootJson = json_decode(gzdecode(File\read($rootFile)), true, 512, JSON_THROW_ON_ERROR);

        foreach ($rootJson['provider-includes'] as $listing => $opts) {
            $listing = str_replace('%hash%', $opts['sha256'], $listing) . '.gz';
            $safeFiles['/' . $listing] = true;

            $listingJson = json_decode(gzdecode(File\read($this->target . '/' . $listing)), true, 512,
                JSON_THROW_ON_ERROR);
            foreach ($listingJson['providers'] as $pkg => $pkgOpts) {
                $provPath = '/p/' . $pkg . '$' . $pkgOpts['sha256'] . '.json.gz';
                $safeFiles[$provPath] = true;
            }
        }

        $this->cleanOldFiles($safeFiles);
    }

    public function statsdIncrement($metric): void
    {
        if ($this->statsdSocket) {
            $this->statsdSocket->write($metric . ':1|c');
        }
    }

    public function output(string $str): void
    {
        if ($this->verbose) {
            $this->outputBuffer .= $str;
        }
    }

    private function getLastModified(HttpMessage $message): int
    {

        if (!$message->hasHeader('last-modified')) {
            throw new \RuntimeException('Expected last-modified header, but did not find one');
        }

        return strtotime($message->getHeader('last-modified'));
    }

    private function downloadV2Files(Pipeline $requests): void
    {
        $concurrency = 256;
        $semaphore = new LocalSemaphore($concurrency);

        while ($request = $requests->continue()) {
            if (defined('EXIT')) {
                $this->output('Found EXIT, exiting...' . PHP_EOL);
                break; // TODO Remove
            }

            $lock = $semaphore->acquire();

            defer(function () use ($request, $lock) {
                $response = $this->client->request($request);

                try {
                    $status = $response->getStatus();
                    if ($status === 304) {
                        $this->output('-');
                        $this->statsdIncrement('mirror.not_modified');
                        return;
                    }

                    if ($status === 404) {
                        // ignore 404s for all v2 files as the package might have been deleted already
                        $this->output('?');
                        $this->statsdIncrement('mirror.not_found');
                        return;
                    }

                    $userData = $response->getOriginalRequest()->getAttribute('user_data');

                    $metadata = $response->getBody()->buffer();
                    if (null === json_decode($metadata, true, 512, JSON_THROW_ON_ERROR)) {
                        throw new \RuntimeException('Invalid JSON received for file ' . $userData['path']);
                    }

                    $mtime = $this->getLastModified($response);
                    $this->write($userData['path'], $metadata, 7, $mtime);
                    $this->output('M');
                    $this->statsdIncrement('mirror.sync_provider_v2');
                } catch (\Throwable $e) {
                    $this->statsdIncrement('mirror.provider_failure');

                    throw $e;
                } finally {
                    $lock->release();
                }
            });
        }

        // Acquire all locks to ensure all requests finished
        $locks = [];
        for ($j = 0; $j < $concurrency; $j++) {
            $locks[] = $semaphore->acquire();
        }
    }

    private function cleanOldFiles(array $safeFiles): void
    {
        $finder = Finder::create()->directories()->ignoreVCS(true)->in($this->target . '/p');
        foreach ($finder as $vendorDir) {
            $vendorFiles = Finder::create()->files()->ignoreVCS(true)
                ->name('/\$[a-f0-9]+\.json\.gz$/')
                ->date('until 10minutes ago')
                ->in((string) $vendorDir);

            foreach ($vendorFiles as $file) {
                $key = strtr(str_replace($this->target, '', $file), '\\', '/');
                if (!isset($safeFiles[$key])) {
                    unlink((string) $file);
                    // also remove the version without .gz suffix if it exists
                    if (File\exists(substr((string) $file, 0, -3))) {
                        File\deleteFile(substr((string) $file, 0, -3));
                    }
                }
            }
        }

        // clean up old provider listings
        $finder = Finder::create()->depth(0)->files()->name('provider-*.json.gz')->ignoreVCS(true)->in($this->target . '/p')->date('until 10minutes ago');
        foreach ($finder as $provider) {
            $key = strtr(str_replace($this->target, '', $provider), '\\', '/');
            if (!isset($safeFiles[$key])) {
                unlink((string) $provider);
                // also remove the version without .gz suffix if it exists
                if (File\exists(substr((string) $provider, 0, -3))) {
                    File\deleteFile(substr((string) $provider, 0, -3));
                }
            }
        }
    }

    private function getHash(string $file): ?string
    {
        if (File\exists($this->target . $file)) {
            return enqueueCallable('hash_file', 'sha256', $this->target . $file);
        }

        return null;
    }

    private function download(string $file): Response
    {
        $response = $this->client->request(new Request($this->url . $file));
        $body = $response->getBody()->buffer(); // trigger exception if needed

        if ($response->getStatus() >= 300) {
            throw new \RuntimeException('Failed to fetch ' . $file . ' => ' . $response->getStatus() . ' ' . $body);
        }

        return $response;
    }

    private function write(string $file, string $content, int $compression, int $mtime): void
    {
        $path = $this->target . $file;

        enqueue(new StorageTask($path, $content, $compression, $mtime, $this->gzipOnly));
    }

    private function delete($packageName): void
    {
        $this->output('D');

        $files = [
            $this->target . '/p2/' . $packageName . '.json',
            $this->target . '/p2/' . $packageName . '.json.gz',
            $this->target . '/p2/' . $packageName . '~dev.json',
            $this->target . '/p2/' . $packageName . '~dev.json.gz',
        ];

        foreach ($files as $file) {
            if (File\exists($file)) {
                File\deleteFile($file);
            }
        }
    }

    private function initClient(): void
    {
        $this->requestCounter = new RequestCounter;

        $this->client = (new HttpClientBuilder)
            ->intercept(new SetRequestHeader('user-agent', $this->userAgent))
            ->intercept(new SetRequestHeader('host', $this->hostname))
            ->intercept(new RetryOnOutdated($this))
            ->intercept(new RetryOnTimeout)
            ->intercept($this->requestCounter)
            ->build();
    }
}
