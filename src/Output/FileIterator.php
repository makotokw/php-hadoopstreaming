<?php
namespace Makotokw\HadoopStreaming\Output;

class FileIterator implements \Iterator
{
    /**
     * @var string
     */
    protected $dir;

    /**
     * @var resource
     */
    protected $handle;

    /**
     * @var string
     */
    protected $key;

    /**
     * @var string
     */
    protected $current;

    public function __construct($dir)
    {
        $this->dir = $dir;
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close()
    {
        if ($this->handle) {
            closedir($this->handle);
            $this->handle = null;
        }
    }

    public function rewind()
    {
        $this->close();

        if (is_dir($this->dir)) {
            $this->handle = opendir($this->dir);
        }
        $this->readDir();
    }

    public function current()
    {
        return $this->current;
    }

    public function key()
    {
        return $this->key;
    }

    public function next()
    {
        $this->readDir();
    }

    public function readDir()
    {
        unset($this->key, $this->current);
        if ($this->handle) {
            while (($file = readdir($this->handle)) !== false) {
                if (preg_match('/^part\-([0-9]+)$/', $file, $matches)) {
                    $this->key = intval($matches[1]);
                    $this->current = $this->dir . DIRECTORY_SEPARATOR . $file;
                    break;
                }
            }
            if (!isset($this->key)) {
                $this->close();
            }
        }
    }

    public function valid()
    {
        return (isset($this->key));
    }
}
