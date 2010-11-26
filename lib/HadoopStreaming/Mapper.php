<?php
abstract class HadoopStreaming_Mapper
{
	var $delimiter,
		$autoSerialize;
	
	function __construct($delimiter = "\t", $autoSerialize = true)
	{
		$this->delimiter = $delimiter;
		$this->autoSerialize = $autoSerialize;
		while (!feof(STDIN)) {
			if (false === $this->map(trim(fgets(STDIN)))) {
				break;
			}
		}
	}
	
	abstract function map($s);
	
	function emit($key, $values)
	{
		if ($this->autoSerialize) {
			echo $key.$this->delimiter.serialize($values).PHP_EOL;
		} else {
			echo $key.$this->delimiter.$values.PHP_EOL;
		}
	}
}
