<?php
/**
 * HadoopStreaming_Output_PartIterator
 * @author makoto_kw
 */
class HadoopStreaming_Output_PartIterator implements Iterator
{
	const TYPE_STDIN = 1;
	const TYPE_FILE = 2;
	
	var $handle,
		$handleType,
		$key,
		$emit,
		$demiliter,
		$autoSerialize;
	
	function __construct($path, $delimiter = "\t", $autoSerialize = true)
	{
		if ($path) {
			if ($this->handle = fopen($path, 'r')) {
				$this->handleType = self::TYPE_FILE;
			}
		} else {
			$this->handle = STDIN;
			$this->handleType = self::TYPE_STDIN;
		}
		$this->delimiter = $delimiter;
		$this->autoSerialize = $autoSerialize;
	}
	
	function __destruct()
	{
		$this->close();
	}
	
	function close()
	{
		if ($this->handle && $this->handleType == self::TYPE_FILE) {
			fclose($this->handle);
			$this->handle = null;
		}
	}
	
	function rewind() {
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
		while ($this->handle && !feof($this->handle)) {
			@list ($key, $value) = explode($this->delimiter, trim(fgets($this->handle)), 2);
			if (!isset($value)) continue;
			if ($this->autoSerialize) {
				$value = unserialize($value);
			}
			$this->key = $key;
			$this->emit = $value;
			break;
		}
	}

	function valid() {
		return (isset($this->key));
	}
}