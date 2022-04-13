from mrjob.job import MRJob

class WordLength(MRJob):

    def mapper(self, key, line):
        words = line.split()
        for w in words:
            length = len(w)
            yield length, 1
    
    def reducer(self, key, values):
        
        yield (key, sum(values))
        

if __name__ == '__main__':
    WordLength.run()
