php-hadoopstreaming
==============

simple test without hadoop

    cd examples/wordcount
    php mapper.php < word.txt | sort | php reducer.php


with hadoop streaming

    hadoop-standalone/bin/hadoop jar hadoop-standalone/hadoop-streaming.jar\
     -input examples/wordcount/word.txt\
     -output examples/wordcount/output\
     -mapper 'php examples/wordcount/mapper.php'\
     -reducer 'php examples/wordcount/reducer.php'


LICENSE
=========

The MIT License (MIT)  
See also LICENSE file