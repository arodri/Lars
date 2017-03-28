from lars.mapper import Mapper
import time

class Sleep(Mapper):

	def loadConfigJSON(self,config):
		self.sleep = float(config["sleep"])

	def process(self,record):
                time.sleep(self.sleep)
		return record



