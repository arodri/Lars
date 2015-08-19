from lars.mapper import Mapper
from datetime import datetime as dt
class RemoveField(Mapper):

	def loadConfigJSON(self,config):
		self.removeFields = config["removeFields"]

	def process(self,record):
		for field in self.removeFields:
			if field in record:
				del(record[field])
		return record

