

## Hadoop Streaming using python language

[1] Download hadoop-streaming-2.6.5.jar

https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-streaming/2.6.5


* [Tutorial](https://www.michael-noll.com/tutorials/writing-an-hadoop-mapreduce-program-in-python/)

* Create mapper.py and reducer.py code (be careful about python indentation)

[2] mapper.py

```
#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        print '%s\t%s' % (word, 1)
```
[3] Reducer.py

```
#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print '%s\t%s' % (current_word, current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%s' % (current_word, current_count)
```

[4] Make mapper.py and reducer.py executable

chmod +x mapper.py
chmod +x reducer.py

[5] Test your code:

```
echo "foo foo quux labs foo bar quux" | ./mapper.py```Output:
```
foo	1
foo	1
quux	1
labs	1
foo	1
bar	1
quux	1
```

echo "foo foo quux labs foo bar quux" | ./mapper.py | sort -k 1,1 | ./reducer.py
```
bar	1
foo	3
labs	1
quux	2
```


```
cat SampleTextFile_1000kb.txt | ./mapper.py 
cat SampleTextFile_1000kb.txt | ./mapper.py | sort -k 1,1
cat SampleTextFile_1000kb.txt | ./mapper.py | sort -k 1,1 | ./reducer.py 
```[6] Copy input file from local file system to HDFS

hadoop fs -copyFromLocal PathToLocalInputfile  PathToHDFSInputdirectory 

[7] Run mapreduce in HDFS using Hadoop Streaming

```
hadoop  jar PathTo_hadoop-streaming.jar \
    -input myInputDirs \
    -output myOutputDir \
    -mapper /bin/cat \
    -reducer /bin/wc
```
example:
```
hadoop jar ~/BigData/hadoop-2.6.5/hadoop-streaming-2.6.5.jar -input /input/SampleTextFile_1000kb.txt -output /output -mapper ~/Documents/Misc/Teaching/CS644BigData/FormalFall2020/Week4/mapper.py -reducer ~/Documents/Misc/Teaching/CS644BigData/FormalFall2020/Week4/reducer.py 
```


