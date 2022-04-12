from mrjob.job import MRJob

class LetterCount(MRJob):

    def mapper_init(self):
        self.cache = {}

    def mapper(self, key, line):
        for symbol in line:
        self.cache[symbol] += 1
        if ( cache size > limit):
            if symbol.isalpha():
                for s in cache:
                    yield (symbol, cache[s])
                cache.clear()

    def mapper_final(self):
        if cache is not empty:
            for s in cache:
                yield s, cache[s]

    def reducer_init(self):
        self.cache = {}

    def reducer(self, key, values):
        yield (key, sum(values))
        

    def reducer_final(self):
        if cache is empty:
            yield reducer

if __name__ == '__main__':
    LetterCount.run()
