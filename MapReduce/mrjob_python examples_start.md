## Examples using mrjob


* 1. create a mrjob mapreduce python file wordcount.py:


```
from mrjob.job import MRJob

class MRWordCount(MRJob):
    '''count word frequency'''
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors="ignore")
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordCount.run()

```

Run the job in local:

> python wordcount.py -r local hdfs:///input/SampleTextFile_1000kb.txt -o output

Run the job in hadoop:

> python wordcount.py -r hadoop hdfs:///input/SampleTextFile_1000kb.txt -o hdfs:///output


* 2. with combiner wordcountcombiner.py:

```
from mrjob.job import MRJob
  
class MRWordCountCombiner(MRJob):
    '''count word frequency with combiner'''
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors="ignore")
            yield word.lower(), 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordCountCombiner.run()
```

> python wordcountcombiner.py -r hadoop hdfs:///input/SampleTextFile_1000kb.txt -o hdfs:///output

