from lars.mapper import Mapper

example_config = """{
	"defaults":[
		{
			"input_field":"some_field",
			"output_fields"[
				"this_fields",
				"this_field_too",
			],
			"default_value":-1
	]
}"""

class DefaultByInput(Mapper):

	def loadConfigJSON(self,config):
		self.defaults = config["defaults"]

	def process(self,record):
		for config in self.defaults:
			if record[config["input_field"]] == "":
				for field in config["output_fields"]:
					record[field] = config["default_value"]
		return record

