from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRmyjob(MRJob): 
    def mapper(self, _, line):
        # extract the data from each line and seperate into variables
        # because there are some lines where the bigrams aren't seperated include a catch for 
        stuff = line.split()
        if(len(stuff) == 6):
            agram,bgram,year,count,page,num_books = stuff
            yield agram+' '+bgram, int(count)
        elif(len(stuff) == 5):
            bigram,year,count,page,num_books = stuff
            yield bigram, int(count)

def reducer(self, bigram, count):
        yield (bigram, sum(count))

if __name__ == '__main__':
	MRmyjob.run()
