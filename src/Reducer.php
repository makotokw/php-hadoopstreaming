<?php
namespace Makotokw\HadoopStreaming;

use Makotokw\HadoopStreaming\Reducer\Iterator as ReducerIterator;

abstract class Reducer
{
    /**
     * @var string
     */
    protected $delimiter = "\t";

    /**
     * @var bool
     */
    protected $autoSerialize = true;

    /**
     * @var string
     */
    protected $inputDelimiter;

    /**
     * @var string
     */
    protected $outputDelimiter;

    /**
     * @var bool
     */
    protected $inputAutoSerialize;

    /**
     * @var bool
     */
    protected $outputAutoSerialize;

    /**
     * @param array $options
     */
    public function __construct($options = array())
    {
        $this->initialize($options);
        if (!isset($this->inputDelimiter)) {
            $this->inputDelimiter = $this->delimiter;
        }
        if (!isset($this->outputDelimiter)) {
            $this->outputDelimiter = $this->delimiter;
        }
        if (!isset($this->inputAutoSerialize)) {
            $this->inputAutoSerialize = $this->autoSerialize;
        }
        if (!isset($this->outputAutoSerialize)) {
            $this->outputAutoSerialize = $this->autoSerialize;
        }
        $iterator = new ReducerIterator($this->inputDelimiter, $this->inputAutoSerialize);
        foreach ($iterator as $key => $emits) {
            if ($values = $this->reduce($key, $emits)) {
                $this->emit($key, $values);
            }
            unset($key, $values, $values);
        }
    }

    /**
     * @param array $options
     */
    protected function initialize($options = array())
    {
    }

    /**
     * reduce
     * @param string $key
     * @param array $emits
     */
    abstract protected function reduce($key, $emits);

    /**
     * @param $key
     * @param $values
     */
    protected function emit($key, $values)
    {
        if ($this->outputAutoSerialize) {
            echo $key . $this->outputDelimiter . serialize($values) . PHP_EOL;
        } else {
            echo $key . $this->outputDelimiter . $values . PHP_EOL;
        }
    }
}
