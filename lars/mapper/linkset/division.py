from mapper import Mapper

class Divide(Mapper):

	def loadConfigJSON(self,config):
		self.divisions = []
		for div_config in config["divisions"]:
			self.divisions.append(SimpleLinksetDivide(div_config))

	def process(self,record):
		for div in self.divisions:
			record = div.process(record)
		return record

class SimpleLinksetDivide:
	def __init__(self,config):
		self.dividend_lst      = config["dividend_template"]
		self.divisor_lst       = config["divisor_template"]
		self.max_size          = config["max_size"]
		self.max_size_result   = config.get("max_size_result", -1.0)
		self.zero_result       = config.get("zero_result", -2.0)
		self.skip_value        = config.get("skip_value", -1)
		self.skip_value_result = config.get("skip_value_result", -3.0)
		self.durations         = config.get("durations",[None])
		self.metrics           = config.get("metrics",[None])
		self.output_lst        = config["output_template"]
		
		# [(dividned,divisor,output)]
		dividends = self.__make_key_strs(self.dividend_lst)
		divisors  = self.__make_key_strs( self.divisor_lst)
		outputs   = self.__make_key_strs(  self.output_lst)

		self.keys = set(zip(dividends, divisors, outputs))

	def __make_key_strs(self,template):
		keys = []
		for duration in self.durations:
			for metric in self.metrics:
				key_parts = {}
				if duration != None:
					key_parts['duration'] = str(duration)
				if metric != None:
					key_parts['metric'] = str(metric)
				keys.append(template % key_parts)
		return keys

	def process(self, record):
		for (dividend, divisor, output) in self.keys:
			if record[dividend] == self.max_size or record[divisor] == self.max_size:
				record[output] = self.max_size_result
			elif record[dividend] == self.skip_value or record[divisor] == self.skip_value:
				record[output] = self.skip_value_result
			elif record[divisor] == 0:
				record[output] = self.zero_result
			else:
				record[output] = record[dividend]/float(record[divisor])
		
		return record
