from lars.mapper import Mapper

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

class NumericToBool(Mapper):
	def loadConfigJSON(self,config):
		self.true_output_value = config.get("true_output_value",1)
		self.false_output_value = config.get("false_output_value",0)
		self.error_output_value = config.get("error_output_value",-1)

		self.fields = []
		self.provides = []
		for field in config["fields"]:
			input_field  = field["input"]
			output_field = field["output"]
			false_value = field.get("false_value",0)
			true_value = field.get("true_value",1) # 1 or greater
			self.fields.append((input_field,output_field,true_value,false_value))
			self.provides = output_field
	
	def process(self, record):
		for (input_field,output_field,true_value,false_value) in self.fields:
			if record[input_field] >= true_value:
				record[output_field] = self.true_output_value # found
			else:
				record[output_field] = self.false_output_value # "error"
		return record



