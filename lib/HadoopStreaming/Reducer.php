<?php
require_once(dirname(__FILE__).'/Reducer/Iterator.php');
/**
 * HadoopStreaming_Reducer
 * @author makoto_kw
 */
abstract class HadoopStreaming_Reducer
{
	var $delimiter = "\t";
	var $autoSerialize = true;
	var $inputDelimiter;
	var $outputDelimiter;
	var $inputAutoSelialize;
	var $outputAutoSerialize;
	
	function __construct()
	{
		if (!isset($this->inputDelimiter)) $this->inputDelimiter = $this->delimiter;
		if (!isset($this->outputDelimiter)) $this->outputDelimiter = $this->delimiter;
		if (!isset($this->inputAutoSelialize)) $this->inputAutoSelialize = $this->autoSerialize;
		if (!isset($this->outputAutoSerialize)) $this->outputAutoSerialize = $this->autoSerialize;
		$iterator = new HadoopStreaming_Reducer_Iterator($this->inputDelimiter, $this->inputAutoSelialize);
		foreach ($iterator as $key => $emits) {
			if ($values = $this->reduce($key, $emits)) {
				$this->emit($key, $values);
			}
			unset($key, $values, $values);
		}
	}
	
	/**
	 * reduce
	 * @param string $key
	 * @param array $emits
	 */
	abstract function reduce($key, $emits);
	
	function emit($key, $values)
	{
		if ($this->outputAutoSerialize) {
			echo $key.$this->outputDelimiter.serialize($values).PHP_EOL;
		} else {
			echo $key.$this->outputDelimiter.$values.PHP_EOL;
		}
	}
}