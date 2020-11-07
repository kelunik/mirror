<?php

namespace Composer\Mirror;

use Amp\CancellationToken;
use Amp\Http\Client\ApplicationInterceptor;
use Amp\Http\Client\DelegateHttpClient;
use Amp\Http\Client\Request;
use Amp\Http\Client\Response;
use Amp\Promise;
use function Amp\delay;

class RetryOnOutdated implements ApplicationInterceptor
{
    /** @var Mirror */
    private $mirror;

    public function __construct(Mirror $mirror)
    {
        $this->mirror = $mirror;
    }

    public function request(Request $request, CancellationToken $cancellation, DelegateHttpClient $httpClient): Response
    {
        $userData = $request->hasAttribute('user_data') ? $request->getAttribute('user_data') : null;

        $attempt = 0;
        $attemptLimit = 3;

        do {
            $lastAttempt = ++$attempt === $attemptLimit;

            $response = $httpClient->request($request, $cancellation);

            if (!$userData) {
                return $response;
            }

            $is404 = $response->getStatus() === 404;
            if ($is404) {
                if ($lastAttempt) {
                    // 404s after 3 retries should be deemed to have really been deleted, so we stop retrying
                    return $response;
                }

                $this->mirror->output('R');
                $this->mirror->statsdIncrement('mirror.retry_provider_v2');

                delay(2000);

                continue;
            }

            $mtime = strtotime($response->getHeader('last-modified'));
            if ($mtime < $userData['minimumFilemtime']) {
                if ($lastAttempt) {
                    // got an outdated file, possibly fetched from a mirror which was not yet up to date
                    throw new \Exception('Too many retries, could not update ' . $userData['path'] . ' as the origin server returns an older file (' . $mtime . ', expected ' . $userData['minimumFilemtime'] . ')');
                }

                $this->mirror->output('R');
                $this->mirror->statsdIncrement('mirror.retry_provider_v2');

                delay(2000);

                continue;
            }

            return $response;
        } while ($attempt < $attemptLimit);

        return $response;
    }
}
