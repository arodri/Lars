from mapper import Mapper
import copy

class MapDistinct(Mapper):
	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.distinct_fields = config["distinct_fields"]
		self.record_lists = config["record_lists"]
		
		self.provides = []
		self.mapped = {}
		for rl in self.record_lists:
			for f in self.distinct_fields:
				self.provides.append("%s_DISTCNT_%s" % (rl, f))
				self.mapped[(rl,f)] = {}
	def process(self,record):
		# dict to hold the histogram
		mapped = copy.deepcopy(self.mapped)
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
		
		self.provides = []
		self.uniq_vals = {}
		for rl in self.record_list:
			for f in self.distinct_fields:
				self.provides.append("%s_uniq_%s" % (rl, f))
				self.uniq_vals[(rl,f)] = set()

		
	def process(self,record):
		unique_vals = copy.deepcopy(self.uniq_vals)
		exclude_values = set()
		for ef in self.exclude_field_values:
			exclude_values.add(record[ef])

		for rl in self.record_list:
			for rl_rec in record[rl]:
				for f in self.distinct_fields:
					v = rl_rec[rl][f]
					if v not in exclude_values:
						unique_vals[(rl,f)].add(v)

		for rl in self.record_list:
			for f in self.distinct_fields:
				record["%s_uniq_%s" % (rl, f)] = len(unique_vals[(rl,f)])
		return record
