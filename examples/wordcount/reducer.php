#!/usr/bin/php
<?php
require_once dirname(__FILE__) . '/../../lib/HadoopStreaming/Reducer.php';
require_once dirname(__FILE__) . '/../../lib/HadoopStreaming/Reducer/Iterator.php';

class Reducer extends \HadoopStreaming\Reducer
{
    protected function reduce($key, $emits)
    {
        $this->emit($key, count($emits));
    }
}
$reducer = new Reducer();
