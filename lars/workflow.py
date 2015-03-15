import importlib
import sys
import json
import logging
import sys
import time

import mapper 
from outputter import * 
#todo- make class vars instead of strings

class Workflow(object):
	
	def __init__(self):
		self.logger = logging.getLogger('workflow')
		self.timing = False
		self.exceptOnMapperError = True

	def buildJSON(self, config,instanceID=None):
		wf= config['workflow']
		self.mappers = []
		self.mapperDict = {}
		try:
			for mapperConfig in wf['mappers']:
				thisMap = mapper.JSONMapperBuilder.buildFromJSON(mapperConfig)
				mapperTup = (thisMap,[])
				self.mappers.append(mapperTup)
				self.mapperDict[thisMap.name] = mapperTup
				self.logger.info("Parsed %s" % thisMap.name)
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
		except mapper.MapperConfigurationException, e:
			self.logger.error(e)
			sys.exit(1)

	def enableTiming(self):
		self.timing = True
	def disableTiming(self):
		self.timing = False
	def enableExceptionHandling(self):
		self.exceptOnMapperError = False
	def disableExceptionHandling(self):
		self.exceptOnMapperError = True
			

	def stop(self):
		for (mapper,outputts) in self.mappers:
			mapper.stop()

	def process(self,record):
		start = time.time()
		if self.mappers == None:
			raise Exception("not built")
		thisRec = record
		
		start = time.time()
		i=0
		for mapper,outputters in self.mappers:
			self.logger.debug("Sending to %s" % mapper.name)
			thisRec = mapper.processWrapper(thisRec,True,False)
			self.logger.debug("Done with %s" % mapper.name)

			if i == len(self.mappers) - 1:
				end = time.time()
				thisRec['TOTAL_TIME'] = ((end-start)*1000)
			for outputter in outputters:
				outputter.output(thisRec)
			i+=1
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
			res = wf.process(json.loads(line))
			recs.append(res)


