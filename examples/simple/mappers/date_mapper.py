from mapper import Mapper
from datetime import datetime as dt
class ObsDateMapper(Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.dateField = "observationDate"
		self.linksets = config["linksetsName"]
		self.obsDateOutput = config["obsDateField"]
		self.obsDateFmt = config.get("obsDateFormat","%Y%m%d")
		

	def run(self,record):
		record[self.obsDateOutput] = dt.strptime(record[self.dateField],self.obsDateFmt)
		for lsName,ls in record[self.linksets].items():
			for rec in ls:
				rec[self.obsDateOutput] = dt.strptime(rec[self.dateField],self.obsDateFmt)
		return record
			
			


