from mapper import Mapper
import json
import logging

def jsonOutput(logOutput):
	return json.dumps(logOutput)

def listOutput(logOutput):
	return str(logOutput.items())

class LoggerMapper(Mapper):

	def loadConfigJSON(self,config):
		fields = config["fields"]
		#logLevel = config.get("log_level")
		self.fieldMap = {}
		for field in fields:
			self.fieldMap[field["to"]] = field["from"]
		logType = config.get("logType","json")
		if logType == "json":
			self.outputHandler = jsonOutput
		elif logType == "list":
			self.outputHandler = listOutput
		else:
			raise Exception("Invalid log type specified - %s" % logType)
		logLevel = config.get("logLevel","debug").lower()
		if logLevel == "critical":
			self.logLevel = logging.CRITICAL
		elif logLevel == "error":
			self.logLevel = logging.ERROR
		elif logLevel == "warning":
			self.logLevel = logging.WARNING
		elif logLevel == "info":
			self.logLevel = logging.INFO
		elif logLevel == "debug":
			self.logLevel = logging.DEBUG
		else:
			raise Exception("Invalid log level set - %s" % logLevel)
		
							


	def process(self,record):
		logOutput = {}
		for logName,recordName in self.fieldMap.items():
			logOutput[logName] = record[recordName]
		self.logger.log(self.logLevel,self.outputHandler(logOutput))
		return record
			
			


