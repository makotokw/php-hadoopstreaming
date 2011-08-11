<?php
/**
 * HadoopStreaming_Reducer_Iterator
 * @author makoto_kw
 */
class HadoopStreaming_Reducer_Iterator implements Iterator
{
    var $key,
        $emits,
        $hasNext,
        $nextKey,
        $nextEmit,
        $demiliter,
        $autoSerialize;

    function __construct($delimiter = "\t", $autoSerialize = true)
    {
        $this->delimiter = $delimiter;
        $this->autoSerialize = $autoSerialize;
    }

    function rewind() {
        $this->key = null;
        $this->nextKey = null;
        $this->nextEmit = null;
        $this->aggregateEmit();
    }

    function current() {
        return $this->emits;
    }

    function key() {
        return $this->key;
    }

    function next() {
        $this->aggregateEmit();
    }

    function aggregateEmit() {
        unset($this->key, $this->emits);
        if (isset($this->nextKey)) {
            $this->key = $this->nextKey;
            $this->emits = array($this->nextEmit);
            unset($this->nextKey, $this->nextEmit);
        } else {
            $this->emits = array();
        }
        while (!feof(STDIN)) {
            @list ($key, $value) = explode($this->delimiter, trim(fgets(STDIN)), 2);
            if (!isset($value)) continue;
            if ($this->autoSerialize) {
                $value = unserialize($value);
            }
            if (!isset($this->key)) {
                $this->key = $key;
                $this->emits[] = $value;
            } else {
                if ($this->key != $key) {
                    $this->nextKey = $key;
                    $this->nextEmit = $value;
                    break;
                } else {
                    $this->emits[] = $value;
                }
            }
        }
    }

    function valid() {
        return (isset($this->key));
    }
}