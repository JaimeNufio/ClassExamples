from mrjob.job import MRJob

class MRFriendsByAge(MRJob):
    '''average number of friends per age'''
    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',')
        yield age, float(numFriends)
        #   (age, numFriends) PER PERSON

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1
            
        yield age, total / numElements

        #   basically, add up the number of friends attached to each age group
        #   and divide by that n


if __name__ == '__main__':
    MRFriendsByAge.run()

