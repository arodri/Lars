from mapper import Mapper
from datetime import datetime as dt
class CopyFields(Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]

		self.copy_fields = []
		copy_fields = config["fields"]
		for d in copy_fields:
			self.copy_fields.append((d["from"],d["to"]))
		

	def process(self,record):
		for copy_from,copy_to in self.copy_fields:
			if copy_from in record:
				record[copy_to] = record[copy_from]
		return record

