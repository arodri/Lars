import json
from mapper import Mapper

class TestMapper(Mapper):
	
	def loadConfigJSON(self,config):
		self.linksetsFieldName = config["linksetsName"]

	def run(self, request):
		for lsName,ls in request[self.linksetsFieldName].items():
			request[lsName+"_cnt"] = len(ls)
		return request

		
