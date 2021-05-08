from mrjob.job import MRJob

class MRWordCount(MRJob):
    '''count word frequency'''
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            word = unicode(word, "utf-8", errors="ignore")
            yield word.lower(), 1
    #mapper takes in each line, then we split it, and yield every word like (foo,1)

    def reducer(self, key, values):
        yield key, sum(values) #<- values is an array of numbers (1,1,1,1...) 
    # then we yield (foo,#appearances)


if __name__ == '__main__':
    MRWordCount.run()

