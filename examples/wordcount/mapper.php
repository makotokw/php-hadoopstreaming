#!/usr/bin/php
<?php
require_once __DIR__ . '/../../src/Mapper.php';

class Mapper extends \Makotokw\HadoopStreaming\Mapper
{
    public function map($s)
    {
        foreach (preg_split('/\s+/', $s) as $word) {
            if ($word !== '') {
                $this->emit($word, 1);
            }
        }
    }
}
$mapper = new Mapper();
