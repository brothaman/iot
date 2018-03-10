from mrjob.job import MRJob
from mrjob.step import MRStep

class MRmyjob(MRJob):
        def getgram(self, wordlist, n):
            for i in range(n, len(wordlist)):
                gram = wordlist[0:n]
                print gram
                del(wordlist[0])
	def mapper(self, _, line):
            n = 2
	    wordlist = line.split()
            for i in range(n, len(wordlist)):
                gram = wordlist[0:n]
                del(wordlist[0])
                yield gram, 1

	def reducer(self, key, list_of_values):
		yield None, (sum(list_of_values),key)

	def reducer2(self, _, list_of_values):
		yield max(list_of_values)

	def steps(self):
		return [MRStep(mapper=self.mapper, reducer=self.reducer), MRStep( reducer=self.reducer2)]

if __name__ == '__main__':
    MRmyjob.run()
