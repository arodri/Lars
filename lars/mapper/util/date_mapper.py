from lars.mapper import Mapper
from datetime import datetime as dt
from datetime import date
class ObsDateMapper(Mapper):

	def loadConfigJSON(self,config):
		self.recordField = config.get("inputDate", None)
		self.obsDateOutput = config.get("obsDateField",self.recordField)
		self.obsDateFmt = config.get("obsDateFormat","%Y%m%d")
		self.linksets = config.get("linksetsName", None)
		self.linksetField = config.get("linksetInputDate",None)
		self.linksetDateOutput= config.get("linksetDateField",self.linksetField)

		self.cache = {}


	def parseDate(self,s):
		if s in self.cache:
			return self.cache[s]
		else:
			date = dt.strptime(s,self.obsDateFmt)
			self.cache[s] = date
			return date

	def process(self,record):
		if self.recordField != None:
			record[self.obsDateOutput] = self.parseDate(record[self.recordField])
		if self.linksets != None:
			for linkset in self.linksets:
				if record[linkset] is not None:
					for link in record[linkset]:
						link[self.linksetDateOutput] = self.parseDate(link[self.linksetField])
		return record



class DatetimeToDateMapper(Mapper):

	def loadConfigJSON(self,config):
		self.datetimeField = config.get("recordDatetimeField",None)
		self.dateOutput = config.get("recordDateField",self.datetimeField)
		self.linksets = config.get("linksetsName", None)
		self.linksetDatetimeField = config.get("linksetDatetimeField",None)
		self.linksetDateOutput = config.get("linksetDateField",self.linksetDatetimeField)

	def process(self,record):
		if self.datetimeField != None:
			record[self.dateOutput] = record[self.datetimeField].date()
		if self.linksets != None:
			for linkset in self.linksets:
				if record[linkset] is not None:
					for link in record[linkset]:
						link[self.linksetDateOutput] = link[self.linksetDatetimeField].date()
		return record
