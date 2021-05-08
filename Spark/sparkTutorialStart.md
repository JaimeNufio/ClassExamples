## Spark Installation ##

Java 8


source ~/.bash_profile


* install scala

Download scala-2.11.12.tgz from https://www.scala-lang.org/download/2.11.12.html

tar -xvzf scala-2.11.12.tgz

* install spark 

Download spark-2.4.4-bin-hadoop2.6.tgz from https://spark.apache.org/downloads.html

* set up environment

.bash_profile

```
export PATH=$PATH:/Users/yaoshen/BigData/scala-2.11.12/bin
export PATH=$PATH:/Users/yaoshen/BigData/spark-2.4.4-bin-hadoop2.6/bin
```

* open spark shell by type in:

spark-shell


## install pyspark ##

python 2.7

pip install pyspark --user



######

## pyspark examples ##

A. ratings-counter.py

Create rating histogram for movierating data

movierating.data
userID movieID	rating	timestamp


```
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///Users/yaoshen/Documents/Misc/Teaching/CS644BigData/Formal/Week12/pyspark_tutorial/movierating.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
    
sc.stop()
```



1.
help(SparkConf)
class SparkConf(__builtin__.object)
 |  Configuration for a Spark application. 
 
 
2.
help(SparkContext)
class SparkContext(__builtin__.object)
 |  Main entry point for Spark functionality. A SparkContext represents the
 |  connection to a Spark cluster, and can be used to create L{RDD} and
 |  broadcast variables on that cluster.
 
 
3.
help(sc.textFile)
textFile(self, name, minPartitions=None, use_unicode=True) method of pyspark.context.SparkContext instance
    Read a text file from HDFS, a local file system (available on all
    nodes), or any Hadoop-supported file system URI, and return it as an
    RDD of Strings.


4.
help(lines)
class RDD(__builtin__.object)
 |  A Resilient Distributed Dataset (RDD), the basic abstraction in Spark.
 |  Represents an immutable, partitioned collection of elements that can be
 |  operated on in parallel.


4.
help(lines.map)
map(self, f, preservesPartitioning=False) method of pyspark.rdd.RDD instance
    Return a new RDD by applying a function to each element of this RDD.


5.
help(ratings.countByValue)
countByValue(self) method of pyspark.rdd.PipelinedRDD instance
    Return the count of each unique value in this RDD as a dictionary of
    (value, count) pairs.
    


B.

word-count.py



```
from pyspark import SparkConf, SparkContext
  
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///Users/yaoshen/Documents/Misc/Teaching/CS644BigData/Formal/Week12/pyspark_tutorial/book.txt")
## input = sc.textFile("hdfs:///input/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
```
