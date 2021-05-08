from pyspark import SparkConf, SparkContext
  
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)


input = sc.textFile("wordCount/book.txt")
## input = sc.textFile("hdfs:///input/book.txt")

words = input.flatMap(lambda x: x.split())
#Notice the map and reduce
#Map -> spit out individual words as ("foo",1)
#Reduce -> 

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    print(result)
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)