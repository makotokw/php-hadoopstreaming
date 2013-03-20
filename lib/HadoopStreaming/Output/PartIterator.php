<?php
namespace HadoopStreaming\Output;

class PartIterator implements \Iterator
{
    const TYPE_STDIN = 1;
    const TYPE_FILE = 2;

    /**
     * @var resource
     */
    protected $handle;

    /**
     * @var int
     */
    protected $handleType;

    /**
     * @var string
     */
    protected $delimiter;

    /**
     * @var bool
     */
    protected $autoSerialize;

    protected $key;
    protected $emit;

    public function __construct($path, $delimiter = "\t", $autoSerialize = true)
    {
        if ($path) {
            if ($this->handle = fopen($path, 'r')) {
                $this->handleType = self::TYPE_FILE;
            }
        } else {
            $this->handle = STDIN;
            $this->handleType = self::TYPE_STDIN;
        }
        $this->delimiter = $delimiter;
        $this->autoSerialize = $autoSerialize;
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close()
    {
        if ($this->handle && $this->handleType == self::TYPE_FILE) {
            fclose($this->handle);
            $this->handle = null;
        }
    }

    public function rewind()
    {
        $this->readEmit();
    }

    public function current()
    {
        return $this->emit;
    }

    public function key()
    {
        return $this->key;
    }

    public function next()
    {
        $this->readEmit();
    }

    public function readEmit()
    {
        unset($this->key, $this->emit);
        while ($this->handle && !feof($this->handle)) {
            @list ($key, $value) = explode($this->delimiter, trim(fgets($this->handle)), 2);
            if (!isset($value)) {
                continue;
            }
            if ($this->autoSerialize) {
                $value = unserialize($value);
            }
            $this->key = $key;
            $this->emit = $value;
            break;
        }
    }

    public function valid()
    {
        return (isset($this->key));
    }
}
