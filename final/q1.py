from mrjob.job import MRJob


class q1(MRJob):
    def mapper(self, key, line):
        line = line.split("\t") #split lines by tabs
        for i in line: 
            neighbors = 0  #initialize count of neighbors
            if int(i[1]) > 1000:  #if neighbors greater than 1000
                neighbors += 1  #increase neighbor count by 1
                yield(i[0], neighbors)  #yield id, and count of neighbors above 1000 
                
    def reducer(self, key, total):
         yield (key, (sum(total) > 5)) #reduce based on id and sum total neighbors above 1000. Only show values greater than 5
  

	
if __name__ == '__main__':
    q1.run()
