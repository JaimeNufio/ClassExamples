from mrjob.job import MRJob
from mrjob.step import MRStep

class MostRatedMovie(MRJob):
    '''find the most rated movie'''
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    # A) Get movieID
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

#This mapper does nothing; it's just here to avoid a bug in some
#versions of mrjob related to "non-script steps." Normally this
#wouldn't be needed.

    # C) pass (NONE,(5,Star Wars))
    def mapper_passthrough(self, key, value):
        yield key, value

    # B) Reduce... by adding 1 for each movie:rating found, and key
    #       (5 , Star Wars)
    def reducer_count_ratings(self, key, values): #(Star wars, (1,1,1,1,1...})
        yield None, (sum(values), key) #(NONE,(5,Star Wars))

    # D) yield the maximum # of ratings
    # Notation is weird, but the way this works in python is that it will print "Other"
    #based on my notes
    def reducer_find_max(self, key, values): #(NONE,{(5,Star Wars),(10,OTher)})
        yield max(values)

if __name__ == '__main__':
    MostRatedMovie.run()

