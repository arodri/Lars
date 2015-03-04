import importlib
import sys
import json
import logging
from mapper import Mapper
from outputter import * 
#todo- make class vars instead of strings

class Workflow(object):

	def buildJSON(self, config,instanceID=None):
		wf= config['workflow']
		self.mappers = []
		self.mapperDict = {}
		for mapperConfig in wf['mappers']:
			module_name,class_name = mapperConfig["class"].split(".")
			thisMod= importlib.import_module(module_name)
			thisMapClass = getattr(thisMod,class_name)
			thisMap = thisMapClass()
			thisMap.initJSON(mapperConfig)
			thisMap.loadConfigJSON(mapperConfig)
			thisMap.isValid(thisMap)
			mapperTup = (thisMap,[])
			self.mappers.append(mapperTup)
			self.mapperDict[thisMap.name] = mapperTup
		for outConfig in wf['outputters']:
			#default outputter is delimited outputter
			thisOutClass = DelimitedOutputter
			if outConfig.has_key("class"):
				module_name,class_name = outConfig["class"].split(".")
				thisMod= importlib.import_module(module_name)
				thisOutClass = getattr(thisMod,class_name)
			thisOut = thisOutClass()
			thisOut.loadConfigJSON(outConfig,instanceID=instanceID)
			if thisOut.after==None:
				self.mappers[-1][1].append(thisOut)
			else:
				self.mapperDict[thisOut.after][1].append(thisOut)
			

	def run(self,record):
		if self.mappers == None:
			raise Exception("not built")
		thisRec = record
		for mapper,outputters in self.mappers:
			thisRec = mapper.run(thisRec)
			for outputter in outputters:
				outputter.output(thisRec)
		return thisRec




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
			recs.append(res)


