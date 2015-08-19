from lars.mapper import Mapper

class MapDistinct(Mapper):
	def loadConfigJSON(self,config):
		self.distinct_fields = config["distinct_fields"]
		self.record_lists = config["record_lists"]

		self.dict_factory = make_distinct_dict_factory(self.record_lists, self.distinct_fields)
		self.provides = [ "%s_DISTINCT_%s" % (rl, f) for (rl,f) in self.dict_factory().keys() ]

	def process(self,record):
		# dict to hold the histogram
		mapped = self.dict_factory()
		for rl in self.record_lists:
			if record[rl] != None:
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
		self.missing_value = config.get("missing_value", -1)
		
		self.dict_factory = make_distinct_dict_factory(self.record_lists, self.distinct_fields)
		self.provides = [ "%s_uniq_%s" % (rl, f) for (rl,f) in self.dict_factory().keys() ]

	def process(self,record):
		unique_vals = self.dict_factory()
		exclude_values = set()
		for ef in self.exclude_field_values:
			exclude_values.add(record[ef])

		for rl in self.record_lists:
			if record[rl] != None:
				for rl_rec in record[rl]:
					for f in self.distinct_fields:
						v = rl_rec[rl][f]
						if v not in exclude_values:
							unique_vals[(rl,f)][v] = 1

		for rl in self.record_list:
			missing = record[rl] == None
			for f in self.distinct_fields:
				record["%s_uniq_%s" % (rl, f)] = self.missing_value if missing else len(unique_vals[(rl,f)].keys())
		return record

class DistinctCountVariables(Mapper):
	def loadConfigJSON(self,config):
		self.record_lists = config["record_lists"]
		self.field_values = config["field_values"] # {"field":[...values...]}
		self.missing_value = config.get("missing_value",-1)
		self.fields = self.field_values.keys()
		self.dict_factory = make_distinct_dict_factory(self.record_lists, self.field_values.keys())

		for (field,values) in self.field_values.items():
			self.field_values[field] = set(values)

	def process(self, record):
		unique_vals = self.dict_factory()
		for rl in self.record_lists:
			for (f,values) in self.field_values.items():
				unique_vals[(rl,f)] = dict([ (v,0) for v in values ])

		for rl in self.record_lists:
			if record[rl] != None:
				for rl_rec in record[rl]:
					for f in self.fields:
						v = rl_rec[f]
						if v in self.field_values[f]:
							unique_vals[(rl,f)][v] += 1
		for ((rl,field),values) in unique_vals.items():
			for (value,cnt) in values.items():
				record["%s_field_%s_value_%s_cnt" % (rl,field,value)] = self.missing_value if record[rl] == None else cnt
		return record

def make_distinct_dict_factory(record_lists, distinct_fields,default_keys=[],initial_value=0):
	def factory():
		mapped = {}
		for rl in record_lists:
			for f in distinct_fields:
				mapped[(rl,f)] = dict([ (k,initial_value) for k in default_keys ])
		return mapped
	return factory

