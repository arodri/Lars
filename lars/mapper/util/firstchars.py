from lars.mapper import Mapper

class FirstChars(Mapper):

    def loadConfigJSON(self,config):
        self.inField = config['fullField']
	self.numChars = config['numChars']
	self.outField = config['outField']

    def process(self,record):
	record[self.outField] = record[self.inField][:self.numChars]
	return record
