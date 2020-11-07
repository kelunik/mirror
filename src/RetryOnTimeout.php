<?php

namespace Composer\Mirror;

use Amp\CancellationToken;
use Amp\Http\Client\ApplicationInterceptor;
use Amp\Http\Client\DelegateHttpClient;
use Amp\Http\Client\Request;
use Amp\Http\Client\Response;
use Amp\Http\Client\TimeoutException;
use Amp\Promise;
use function Amp\call;
use function Amp\delay;

class RetryOnTimeout implements ApplicationInterceptor
{
    public function request(Request $request, CancellationToken $cancellation, DelegateHttpClient $httpClient): Response
    {
        $attempt = 0;
        $attemptLimit = 3;

        do {
            $lastAttempt = ++$attempt === $attemptLimit;

            try {
                $response = $httpClient->request($request, $cancellation);
                $response->getBody()->buffer();

                return $response;
            } catch (TimeoutException $e) {
                if ($lastAttempt) {
                    throw $e;
                }

                delay(1000);
            }
        } while (true);
    }
}
