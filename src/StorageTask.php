<?php

namespace Composer\Mirror;

use Amp\CancellationToken;
use Amp\File;
use Amp\Parallel\Worker\Environment;
use Amp\Parallel\Worker\Task;

final class StorageTask implements Task
{
    private string $path;
    private string $content;
    private int $compression;
    private int $modificationTime;
    private bool $gzipOnly;

    public function __construct(string $path, string $content, int $compression, int $modificationTime, bool $gzipOnly)
    {
        $this->path = $path;
        $this->content = $content;
        $this->compression = $compression;
        $this->modificationTime = $modificationTime;
        $this->gzipOnly = $gzipOnly;
    }

    public function run(Environment $environment, CancellationToken $token)
    {
        $gzipped = gzencode($this->content, $this->compression);

        if (!File\isDirectory(dirname($this->path))) {
            File\createDirectoryRecursively(dirname($this->path), 0777);
        }

        if (!$this->gzipOnly || \str_ends_with($this->path, '/packages.json')) {
            File\write($this->path . '.tmp', $this->content);
            File\touch($this->path . '.tmp', $this->modificationTime);
            File\move($this->path . '.tmp', $this->path);
        }

        File\write($this->path . '.gz.tmp', $gzipped);
        File\touch($this->path . '.gz.tmp', $this->modificationTime);
        File\move($this->path . '.gz.tmp', $this->path . '.gz');
    }
}