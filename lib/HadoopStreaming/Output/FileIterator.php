<?php
/**
 * HadoopStreaming_Output_FileIterator
 * @author makoto_kw
 */
class HadoopStreaming_Output_FileIterator implements Iterator
{
    var $dir;
    var $key;
    var $current;
    var $handle;

    function __construct($dir)
    {
        $this->dir = $dir;
    }

    function __destruct()
    {
        $this->close();
    }

    function close()
    {
        if ($this->handle) {
            closedir($this->handle);
            $this->handle = null;
        }
    }

    function rewind() {
        $this->close();

        if (is_dir($this->dir)) {
            $this->handle = opendir($this->dir);
        }
        $this->readdir();
    }

    function current() {
        return $this->current;
    }

    function key() {
        return $this->key;
    }

    function next() {
        $this->readdir();
    }

    function readdir() {
        unset($this->key, $this->current);
        if ($this->handle) {
            while (($file = readdir($this->handle)) !== false) {
                if (preg_match('/^part\-([0-9]+)$/', $file, $matches)) {
                    $this->key = intval($matches[1]);
                    $this->current = $this->dir.DIRECTORY_SEPARATOR.$file;
                    break;
                }
            }
            if (!isset($this->key)) {
                $this->close();
            }
        }
    }

    function valid() {
        return (isset($this->key));
	}
}