from mrjob.job import MRJob
from mrjob.step import MRStep
import codecs

class MostRatedMoviename(MRJob):
    '''find the most rated movie with its name'''

    def configure_args(self):
        super(MostRatedMoviename, self).configure_args()
        self.add_file_arg('--name', help='Path to moviename.data')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper=self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

#This mapper does nothing; it's just here to avoid a bug in some
#versions of mrjob related to "non-script steps." Normally this
#wouldn't be needed.

    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_init(self):
        self.movieNames = {}
        with codecs.open("moviename.data", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movieNames[key])

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostRatedMoviename.run()

## run
## python mostratedmoviename.py -r local movierating.data --name moviename.data -o output
