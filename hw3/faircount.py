from mrjob.job import MRJob


class FairCount(MRJob):

    def mapper(self, key, line):
        """
        Mapper function which will yield the word's in the file and their totals, 
        all the letters and their totals, and how many lines are in the file
        """
        # find all words in file 
        words = line.split()
        for w in words:
            yield w, 1

        # find all letters in file
        for w in words:
            for letter in w:
                if letter.isalpha():       
                    yield letter+"_", 1

        # find how many lines in file
        for line in line:
            yield ("_lines_", 1)


    def reducer(self, key, values):
        """
        Reducer function which will yield each key found and the sum for each
        """
        yield key, sum(values)

if __name__ == '__main__':
    FairCount.run()


