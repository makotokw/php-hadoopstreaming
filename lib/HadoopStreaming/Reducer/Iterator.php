<?php
namespace HadoopStreaming\Reducer;

class Iterator implements \Iterator
{
    protected $key;
    protected $emits;
    protected $hasNext;
    protected $nextKey;
    protected $nextEmit;

    /**
     * @var string
     */
    protected $delimiter;

    /**
     * @var bool
     */
    protected $autoSerialize;

    public function __construct($delimiter = "\t", $autoSerialize = true)
    {
        $this->delimiter = $delimiter;
        $this->autoSerialize = $autoSerialize;
    }

    public function rewind()
    {
        $this->key = null;
        $this->nextKey = null;
        $this->nextEmit = null;
        $this->aggregateEmit();
    }

    public function current()
    {
        return $this->emits;
    }

    public function key()
    {
        return $this->key;
    }

    public function next()
    {
        $this->aggregateEmit();
    }

    public function aggregateEmit()
    {
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
            if (!isset($value)) {
                continue;
            }
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

    public function valid()
    {
        return (isset($this->key));
    }
}
