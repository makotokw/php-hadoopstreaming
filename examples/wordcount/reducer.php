#!/usr/bin/php
<?php
require_once __DIR__ . '/../../src/Reducer.php';
require_once __DIR__ . '/../../src/Reducer/Iterator.php';

class Reducer extends \Makotokw\HadoopStreaming\Reducer
{
    protected function reduce($key, $emits)
    {
        $this->emit($key, count($emits));
    }
}
$reducer = new Reducer();
