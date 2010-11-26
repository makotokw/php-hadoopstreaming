<?php
class HadoopStreaming_Reducer_Iterator implements Iterator
{
	var $key,
		$lastEmit,
		$emits,
		$demiliter,
		$autoSerialize;
	
	function __construct($delimiter = "\t", $autoSerialize = true)
	{
		$this->delimiter = $delimiter;
		$this->autoSerialize = $autoSerialize;
	}
	
	function rewind() {
		$this->key = null;
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
		$this->emits = array();
		if ($this->nextEmit !== null) {
			$this->emits[] = $this->nextEmit;
			unset($this->nextEmit);
		}
		while (!feof(STDIN)) {
			list ($key, $value) = explode($this->delimiter, trim(fgets(STDIN)), 2);
			if ($this->autoSerialize) {
				$value = unserialize($value);
			}
			if ($this->key === null) {
				$this->key = $key;
				$this->emits[] = $value;
			} else {
				if ($this->key != $key) {
					$this->lastEmit = $value;
					break;
				} else {
					$this->emits[] = $value;
				}
			}
		}
	}

	function valid() {
		return (!feof(STDIN));
	}
}
