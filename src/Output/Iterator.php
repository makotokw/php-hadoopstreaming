<?php
namespace Makotokw\HadoopStreaming\Output;

class Iterator implements \Iterator
{
    /**
     * @var PartIterator
     */
    protected $part;

    /**
     * @var FileIterator
     */
    protected $files;

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

    public function __construct($dir, $delimiter = "\t", $autoSerialize = true)
    {
        $this->files = new FileIterator($dir);
        $this->delimiter = $delimiter;
        $this->autoSerialize = $autoSerialize;
    }

    public function rewind()
    {
        $this->files->rewind();
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

        if (isset($this->part)) {
            $this->part->next();
            if ($this->part->valid()) {
                $this->key = $this->part->key();
                $this->emit = $this->part->current();
                return;
            } else {
                unset($this->part);
                $this->part = null;
            }
        }
        if (!isset($this->part) && $this->files->valid()) {
            if ($file = $this->files->current()) {
                $this->part = new PartIterator($file, $this->delimiter, $this->autoSerialize);
                $this->part->rewind();
                if ($this->part->valid()) {
                    $this->key = $this->part->key();
                    $this->emit = $this->part->current();
                }
            }
            $this->files->next();
        }
    }

    public function valid()
    {
        return (isset($this->key));
    }
}
