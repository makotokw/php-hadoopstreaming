<?php
/**
 * HadoopStreaming_Mapper
 * @author makoto_kw
 */
abstract class HadoopStreaming_Mapper
{
	var $delimiter = "\t";
	var $autoSerialize = true;
	
	function __construct()
	{
		while (!feof(STDIN)) {
			if (false === $this->map(trim(fgets(STDIN)))) {
				break;
			}
		}
	}
	
	/**
	 * map
	 * @param string $s	a line from STDIN
	 */
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