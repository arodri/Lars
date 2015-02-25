import importlib
import sys
import json
import logging
from mapper import Mapper

class Workflow(object):
	


	def buildJSON(self, config):
		wf= config['workflow']
		self.mappers = []
		for mapperConfig in wf['mappers']:
			module_name,class_name = mapperConfig["class"].split(".")
			thisMod= importlib.import_module(module_name)
			thisMapClass = getattr(thisMod,class_name)
			thisMap = thisMapClass()
			thisMap.initJSON(mapperConfig)
			thisMap.loadConfigJSON(mapperConfig)
			thisMap.isValid(thisMap)
			self.mappers.append(thisMap)

	def run(self,record):
		if self.mappers == None:
			raise Exception("not built")
		thisReq = record
		for mapper in self.mappers:
			thisReq = mapper.run(thisReq)
		return thisReq




if __name__ == "__main__":
	logging.basicConfig(level=logging.DEBUG)
        recs = []
	wfJSON = sys.argv[1]
	records = sys.argv[2]
	wf = Workflow()
	with open(wfJSON,'rb') as wfFH:
		wf.buildJSON(json.load(wfFH))
		#print wf.mappers
	with open(records,'rb') as recordFH:
		for line in recordFH:
			res = wf.run(json.loads(line))


