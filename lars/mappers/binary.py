from mapper import Mapper

class BooleanToNumeric(Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.true_value = config["true_value"]
		self.false_value = config["false_value"]

		self.fields = config["fields"]

	def process(self,record):
		for field in self.fields:
			if field in record:
				if record[field] == True:
					record[field] = self.true_value
				elif record[field] == False:
					record[field] = self.false_value
		return record

