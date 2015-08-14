from lars.mapper import Mapper
from datetime import datetime as dt
class ObsDateMapper(Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.dateField = config.get("inputDate", "observationDate")
		self.linksets = config.get("linksetsName", None)
		self.obsDateOutput = config["obsDateField"]
		self.obsDateFmt = config.get("obsDateFormat","%Y%m%d")
		self.skipRecord = config.get("skipRecord",False)
		
		self.cache = {}


	def parseDate(self,s):
		if s in self.cache:
			return self.cache[s]
		else:
			date = dt.strptime(s,self.obsDateFmt)
			self.cache[s] = date
			return date

	def process(self,record):
		if not self.skipRecord:
			record[self.obsDateOutput] = self.parseDate(record[self.dateField])
		if self.linksets != None:
			for lsName,ls in record[self.linksets].items():
				if ls != None:
					for rec in ls:
						rec[self.obsDateOutput] = self.parseDate(rec[self.dateField])
		return record
			
			


