from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):
	def mapper(self, _, line):
		agram,bgram,year,count,page,num_books = line.split()
		yield [agram+' '+bgram], int(count)

	def reducer(self, bigram, count):
		yield None, (bigram, sum(count))

if __name__ == '__main__':
	MRmyjob.run()
