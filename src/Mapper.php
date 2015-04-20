<?php
namespace Makotokw\HadoopStreaming;

abstract class Mapper
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
     * @param array $options
     */
    public function __construct($options = array())
    {
        $this->initialize($options);
        while (!feof(STDIN)) {
            if (false === $this->map(trim(fgets(STDIN)))) {
                break;
            }
        }
    }

    /**
     * @param array $options
     */
    protected function initialize($options = array())
    {
    }

    /**
     * map
     * @param string $s    a line from STDIN
     */
    abstract protected function map($s);

    /**
     * @param $key
     * @param $values
     */
    protected function emit($key, $values)
    {
        if ($this->autoSerialize) {
            echo $key . $this->delimiter . serialize($values) . PHP_EOL;
        } else {
            echo $key . $this->delimiter . $values . PHP_EOL;
        }
    }
}
