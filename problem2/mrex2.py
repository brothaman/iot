"""
Implement a MapReduce program that computes the most common bigram in each year in the
dataset (as determined by the count field).
Output of the program should include: (year, bigram, count)
Example output: (2001, mobile phone, 5002) means that in the year 2001 the most popular bigram
was 'mobile phone' and it appeared 5002 times in all the books in that year. Emit such tuples for
each year in the dataset.

"""

from mrjob.job import MRJob

class MyMRJob(MRJob):
    def mapper(self, _, line):
        data=line.split('\t')
        ngram = data[0].strip()
        year = data[1].strip()
        count = data[2].strip()
        yield year, (int(count),ngram)

    def reducer(self, key, list_of_values):
        yield key, max(list_of_values)
   
if __name__ == '__main__':
    MyMRJob.run()

