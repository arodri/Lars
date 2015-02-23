import json
from mapper import Mapper

class TestMapper2(Mapper):
	
	def loadConfigJSON(self,config):
		self.prefix = config["prefix"]+"_"
		self.linksetsFieldName = config["linksetsName"]

	def run(self, request):
		for lsName,ls in request[self.linksetsFieldName].items():
			request[self.prefix+lsName+"_cnt"] = len(ls)
		return request

		
