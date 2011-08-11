<?php
require_once(dirname(__FILE__).'/FileIterator.php');
require_once(dirname(__FILE__).'/PartIterator.php');

/**
 * HadoopStreaming_Output_Iterator
 * @author makoto_kw
 */
class HadoopStreaming_Output_Iterator implements Iterator
{
    var $part,
        $files,
        $key,
        $emit,
        $delimiter,
        $autoSerialize;

    function __construct($dir, $delimiter = "\t", $autoSerialize = true)
    {
        $this->files = new HadoopStreaming_Output_FileIterator($dir);
        $this->delimiter = $delimiter;
        $this->autoSerialize = $autoSerialize;
    }

    function rewind() {
        $this->files->rewind();
        $this->readEmit();
    }

    function current() {
        return $this->emit;
    }

    function key() {
        return $this->key;
    }

    function next() {
        $this->readEmit();
    }

    function readEmit() {
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
                $this->part = new HadoopStreaming_Output_PartIterator($file, $this->delimiter, $this->autoSerialize);
                $this->part->rewind();
                if ($this->part->valid()) {
                    $this->key = $this->part->key();
                    $this->emit = $this->part->current();
                }
            }
            $this->files->next();
        }
    }

    function valid() {
        return (isset($this->key));
	}
}