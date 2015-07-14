import logging
from mapper import Mapper


class Substring(Mapper):
		
	def loadConfigJSON(self,config):
		self.field = config["field"]
		self.outputField = config["outputField"]
		self.start = config.get("start",0)
		self.size = config["size"]
		self.end =self.start+self.size 

	def process(self,record):
		sub = record[self.field][self.start:self.end]
		record[self.outputField] = sub
		return record

class ConcatFieldsMapper(Mapper):
	badAddKey = "-1"
	

	def loadConfigJSON(self,config):
		#takes a list of fields will concatenate all to make street address
		self.seperator = config.get("seperator","_")
		self.baseFields = config['baseFields']
		self.fieldName = config['outputField']
		self.listField = config["listField"]
		self.listBaseFields = config["listBaseFields"]
		self.listOutputField = config["listOutputField"]


	def process(self,record):
		missing=False
		#this is dirty and only to solve the particular problem that I have
		for field in self.baseFields:
			if record[field] =="":
				missing=True
	
		if not missing:
			val = self.seperator.join([record[field] for field in self.baseFields])
		else:
			val = ""
		record[self.fieldName] = val
		for r in record[self.listField]:
			missing = False
			for field in self.listBaseFields:
				if r[field] =="":
					missing = True
			if not missing:
				thisVal =self.seperator.join([r[field] for field in self.listBaseFields])
			else:
				thisVal = ""
			r[self.listOutputField] = thisVal			

		return record
			
class ConstantMapper(Mapper):

	def loadConfigJSON(self,config):
		self.map = config["map"]
		self.overwrite = config.get("overwrite", True)

	def process(self,record):
		for key,val in self.map.items():
			if self.overwrite or key not in record:
				record[key] = val
		return record
