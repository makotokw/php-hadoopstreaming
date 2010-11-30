#!/usr/bin/php
<?php
require_once dirname(__FILE__).'/../../lib/HadoopStreaming/Reducer.php';
class Reducer extends HadoopStreaming_Reducer
{
	function reduce($key, $emits)
	{
		$this->emit($key, count($emits));
	}
}
$reducer = new Reducer();
