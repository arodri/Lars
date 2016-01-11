from lars.mapper import Mapper
import json 

class InsertJSONFileMapper(Mapper):
	def loadConfigJSON(self, config):
		self.file = config['file']
		self.keyToInsert = config['keyToInsert']
		self.outputKey = config['outputKey']

		with open(self.file,'rb') as f:
			self.data = json.load(f)

	def process(self, record):
		record[self.outputKey] = self.data[self.keyToInsert]
		return record


