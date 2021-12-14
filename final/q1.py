from mrjob.job import MRJob


class q1(MRJob):
    def mapper(self, key, line):
        line = line.split("\t")
        for i in line:
            neighbors = 0
            if int(i(1)) > 1000:
                neighbors += 1
                yield(i, neighbors)
                
    def reducer(self, key, total):
         yield (key, (sum(total) > 5))
  

	
if __name__ == '__main__':
    q1.run()
