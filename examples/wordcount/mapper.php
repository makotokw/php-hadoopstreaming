#!/usr/bin/php
<?php
require_once dirname(__FILE__) . '/../../lib/HadoopStreaming/Mapper.php';

class Mapper extends \HadoopStreaming\Mapper
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