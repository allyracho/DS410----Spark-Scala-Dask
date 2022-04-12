import csv
from mrjob.job import MRJob


class retail(MRJob):

	def mapper(self, key, s):
		"""
		Mapper function which will calculate the total for each item, 
		and yield the item description (key) and found total 
		"""
		# read line
		parts = list(csv.reader([s]))[0]
		
		# remove white space and replace empty space with NA
		parts[2] = parts[2].strip()

		for i in parts[2]:
			if len(i) == 0:
				i = 'NA'

		# as long as line is not header, calculte total of each item, 
		# yield description and total
		if parts[3] != 'Quantity':
			quantity = int(parts[3])
			price = float(parts[5])
			total = quantity*price

			yield (parts[2], total)
		
	def reducer(self, key, total):
		"""
		Reducer function which will output our key and sum the totals 
		found for each item
		"""
		yield key, sum(total)
		
	
if __name__ == '__main__':
    retail.run()


