#!/usr/bin/env python
import importlib
import sys
import json
import logging
import loggingAdapters
import sys
import time
import argparse
import socket

from feeder import FileReader
import mapper 
from outputter import *
from record import record
#todo- make class vars instead of strings
#TODO: Name the workflows so that you can auto assign record_ids in a multi-threaded environment

class Workflow(object):
	
	def __init__(self):
		logger = logging.getLogger('workflow')
		self.timing = False
		self.exceptOnMapperError = True
		self.numProc = 0
		self.context = {"hostname":socket.gethostname(),"record_id":None,"record":None}
		self.logger = loggingAdapters.WorkflowLoggerAdapter(logger,self.context)

	def buildJSON(self, config,instanceID=None):
		wf= config['workflow']
		self.getRecordIDField = wf.get("get_recordID_field",None)
		self.putRecordIDField = wf.get("put_recordID_field",None)
		self.mappers = []
		self.mapperDict = {}
		try:
			for mapperConfig in wf['mappers']:
				thisMap,skip = mapper.JSONMapperBuilder.buildFromJSON(mapperConfig)
				if skip:
					self.logger.info("skipping %s" % thisMap.name)
				else:
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

	def process(self,recordDict):
		thisRec = record(recordDict)
		#if a record_id is provided in the original dict use that, else use the 
		#number of records that have been processed thus far
		if self.getRecordIDField:
			try:
				thisRec.set_record_id(thisRec[self.getRecordIDField])
			except KeyError:
				raise KeyError("recordID cannot be assigned, %s not present in input" % self.getRecordIDField) 
		else:
			thisRec.set_record_id(self.numProc)
		#assign the record_id to a field in the underlying dict if requested
		if self.putRecordIDField:
			thisRec[self.putRecordIDField] = thisRec.get_record_id()
		#change the context for logging 
		self.context["record_id"] = thisRec.get_record_id()
		thisRec.set_context(self.context)
		start = time.time()
		if self.mappers == None:
			raise Exception("not built")
		
		start = time.time()
		i=0
		#run through all the mappers and outputters
		for mapper,outputters in self.mappers:
			self.logger.debug("Sending to %s" % mapper.name)
			thisRec = mapper.processWrapper(thisRec,True,self.exceptOnMapperError)
			self.logger.debug("Done with %s" % mapper.name)

			if i == len(self.mappers) - 1:
				end = time.time()
				thisRec['TOTAL_TIME'] = ((end-start)*1000)
			for outputter in outputters:
				outputter.output(thisRec)
			i+=1
		#increment the processed records
		self.numProc+=1
		return thisRec




if __name__ == "__main__":

	parser = argparse.ArgumentParser(description='Simple single-threaded interface for running a workflow')

	parser.add_argument('input_file', nargs=1, type=argparse.FileType('r'), help='Input file. Either delimited with a header OR 1-JSON-per-line')
	parser.add_argument('workflow', nargs=1, type=argparse.FileType('r'), help='Workflow JSON configuration.')
	parser.add_argument('-d', default='|', metavar='DELIM', help='Input file delimiter. If the file is already JSON format (one line per record) specify "JSON". (default: %(default)s)')

	args = parser.parse_args()

	logging.basicConfig(level=logging.DEBUG)
	
	wf = Workflow()
	with args.workflow[0] as wfFH:
		wf.buildJSON(json.load(wfFH))

	with args.input_file[0] as recordFH:
		reader = FileReader(recordFH, args.d)
		for r in reader:
			logging.debug(wf.process(r))


