from lars.mapper import Mapper
import json 

class NestedJSONMapper(Mapper):
	def loadConfigJSON(self, config):
		self.outputKey = config['outputKey']
		self.fieldsToInsert = config['fieldsToInsert']
		self.nested = {}

	def process(self, record):
		for field in self.fieldsToInsert:
			self.nested[field] = record[field]
		record[self.outputKey] = self.nested
		return record


