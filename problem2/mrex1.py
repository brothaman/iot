'''
Implement a MapReduce program that computes the most popular bigram (2-gram) of all time in 
the dataset (as determined by the count field).  
'''
from mrjob.job import MRJob
from mrjob.step import MRStep

class MyMRJob(MRJob): 
    def mapper(self, _, line): 
        data=line.split('\t') 
        ngram = data[0].strip() 
        year = data[1].strip() 
        count = data[2].strip() 
        pages = data[3].strip() 
        books = data[4].strip() 
        #Emit key-value pairs where key is ngram+year and value is count of ngram 
        yield ngram+year, int(count) 
		
    def reducer(self, key, list_of_values): 
        # Send all (count, ngram+year) pairs to the same reducer. 
        # So we can easily use Python's max() function. 
        yield None, (sum(list_of_values),key) 
		
    def reducer2(self, _, list_of_values): 
        # Reducer-2 get input tuples as follows: 
        # None, [(212, cloud computing 2006), (156, mobile phones 2003)] 
        # max function will yield tuple with max value of the count 
        yield max(list_of_values) 
		
    def steps(self): 
        return [MRStep(mapper=self.mapper, reducer=self.reducer), MRStep(reducer=self.reducer2)] 

if __name__ == '__main__': 
    MyMRJob.run() 
