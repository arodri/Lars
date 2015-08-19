import json

from lars.mapper import Mapper


class ResponseMapper(Mapper):

	def loadConfigJSON(self,config):
		fmapJSON = config["field_map"]
		fieldsFH = open(fmapJSON,'r')
		self.fieldMapping = json.load(fieldsFH)
		fieldsFH.close()
		


	def process(self, record):
		response = {}
		for (responseName,recordName) in self.fieldMapping.items():
			response[responseName] = record[recordName]
		return response

