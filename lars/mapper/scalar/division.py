from lars.mapper import Mapper

class Divide(Mapper):

	def loadConfigJSON(self,config):
		self.divisions = []
		for div_config in config["divisions"]:
			self.divisions.append(SimpleDivide(div_config))

	def process(self,record):
		for div in self.divisions:
			record = div.process(record)
		return record

class SimpleDivide:
	def __init__(self,config):
		self.dividend             = config["dividend"]
		self.divisor              = config["divisor"]
		self.output               = config["output"]
		self.invalid_values       = config.get("invalid_values", [])
		self.invalid_value_result = config.get("invalid_value_result", -1.0)
		self.zero_result          = config.get("zero_result", -2.0)
		self.skip_values          = config.get("skip_values", [-1])
		self.skip_value_result    = config.get("skip_value_result", -3.0)
		self.durations            = config.get("durations",[None])
		self.metrics              = config.get("metrics",[None])

		self.invalid_values = set(self.invalid_values)
		self.skip_values = set(self.skip_values)

	def process(self, record):
		if record[self.dividend] in self.invalid_values or record[self.divisor] in self.invalid_values:
			record[output] = self.invalid_value_result
		elif record[self.dividend] in self.skip_values or record[self.divisor] in self.skip_values:
			record[output] = self.skip_value_result
		elif record[self.divisor] == 0:
			record[self.output] = self.zero_result
		else:
			record[self.output] = record[self.dividend]/float(record[self.divisor])
		
		return record
