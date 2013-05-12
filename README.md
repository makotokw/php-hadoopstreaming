php-hadoopstreaming
==============

## Install

Add to composer.json

    {
        "require": {
            "makotokw/hadoopstreaming": "dev-master"
        }
    }


## Usage

simple test without hadoop

    cd examples/wordcount
    php mapper.php < word.txt | sort | php reducer.php


with hadoop streaming

    hadoop-standalone/bin/hadoop jar hadoop-standalone/hadoop-streaming.jar\
     -input examples/wordcount/word.txt\
     -output examples/wordcount/output\
     -mapper 'php examples/wordcount/mapper.php'\
     -reducer 'php examples/wordcount/reducer.php'


## LICENSE

The MIT License (MIT)  
