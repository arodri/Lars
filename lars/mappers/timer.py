from mapper import Mapper
import time

class StartTimer(Mapper):

	def loadConfigJSON(self,config):
		self.outputField = config["outputField"]

	def process(self,record):
		record[self.outputField] = time.time()
		return record

class EndTimer(Mapper):
	def loadConfigJSON(self,config):
		self.outputField = config["outputField"]
	
	def process(self,record):
		ms = -1
		if self.outputField in record:
			ms = ((time.time() - record[self.outputField])*1000)
		record[self.outputField] = ms
		return record


