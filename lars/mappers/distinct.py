from mapper import Mapper

class MapDistinct(Mapper):
	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.distinct_fields = config["distinct_fields"]
		self.record_lists = config["record_lists"]

		self.dict_factory = make_distinct_dict_factory(self.record_lists, self.distinct_fields)
		self.provides = [ "%s_DISTCNT_%s" % (rl, f) for (rl,f) in self.dict_factory().keys() ]

	def process(self,record):
		# dict to hold the histogram
		mapped = self.dict_factory()
		for rl in self.record_lists:
			for rl_rec in record[rl]:
				for f in self.distinct_fields:
					mapped[(rl,f)][rl_rec[f]] = mapped[(rl,f)].get(rl_rec[f],0) + 1
		for rl in self.record_lists:
			for f in self.distinct_fields:
				record["%s_DISTINCT_%s" % (rl,f)] = mapped[(rl,f)].items()
		return record

class DistinctCount(Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.distinct_fields = config["distinct_fields"]
		self.record_lists = config["record_lists"]
		self.exclude_field_values = config["exclude_field_values"]
		
		self.dict_factory = make_distinct_dict_factory(self.record_lists, self.distinct_fields)
		self.provides = [ "%s_uniq_%s" % (rl, f) for (rl,f) in self.dict_factory().keys() ]

	def process(self,record):
		unique_vals = self.dict_factory()
		exclude_values = set()
		for ef in self.exclude_field_values:
			exclude_values.add(record[ef])

		for rl in self.record_list:
			for rl_rec in record[rl]:
				for f in self.distinct_fields:
					v = rl_rec[rl][f]
					if v not in exclude_values:
						unique_vals[(rl,f)][v] = 1

		for rl in self.record_list:
			for f in self.distinct_fields:
				record["%s_uniq_%s" % (rl, f)] = len(unique_vals[(rl,f)].keys())
		return record

def make_distinct_dict_factory(record_lists, distinct_fields):
	def factory():
		mapped = {}
		for rl in record_lists:
			for f in distinct_fields:
				mapped[(rl,f)] = {}
		return mapped
	return factory

