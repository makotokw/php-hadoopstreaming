<?php
require_once(dirname(__FILE__).'/Reducer/Iterator.php');
abstract class HadoopStreaming_Reducer
{
	var $delimiter,
		$autoSerialize;
		
	function __construct($delimiter = "\t", $autoSerialize = true)
	{
		$this->delimiter = $delimiter;
		$this->autoSerialize = $autoSerialize;
		$iterator = new HadoopStreaming_Reducer_Iterator($delimiter, $autoSerialize);
		foreach ($iterator as $key => $eimits) {
			if ($values = $this->reduce($key, $eimits)) {
				$this->emit($key, $values);
			}
		}
	}
	abstract function reduce($key, $eimits);
	
	function emit($key, $values)
	{
		if ($this->autoSerialize) {
			echo $key.$this->delimiter.serialize($values).PHP_EOL;
		} else {
			echo $key.$this->delimiter.$values.PHP_EOL;
		}
	}
}