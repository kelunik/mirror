<?php

namespace Composer\Mirror;

use Amp\CancellationToken;
use Amp\Http\Client\ApplicationInterceptor;
use Amp\Http\Client\DelegateHttpClient;
use Amp\Http\Client\Request;
use Amp\Http\Client\Response;

class RequestCounter implements ApplicationInterceptor
{
    /** @var int */
    private $requestCount = 0;

    public function request(Request $request, CancellationToken $cancellation, DelegateHttpClient $httpClient): Response
    {
        $this->requestCount++;

        return $httpClient->request($request, $cancellation);
    }

    public function getRequestCount(): int
    {
        return $this->requestCount;
    }
}