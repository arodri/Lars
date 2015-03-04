import sys
import json

class Outputter:
	_AFTER="after"

	def loadConfigJSON(self,config,instanceID=None):
		self.initJSON(config)

	def initJSON(self,config):
		self.after = config.get(Outputter._AFTER,None)	

	def output(self,record):
		raise NotImplementedError(" Should have implemented this")

class OutputterConfigurationException(Exception):
	pass

class DelimitedOutputter(Outputter):

	_FMTFILE="formatFile"
	_FIELD_NAMES="fields"
	_DELIM="delimiter"
	_DEF_DELIM="|"
	_OUTPUT_FILE="outputFile"
	_STDOUT="-"

	def loadConfigJSON(self,config,instanceID=None):
		self.initJSON(config)
		isGood=False

		self.fields = []
		fieldNames = []
		#check to ensure has a fmtFile or list of fields
		if config.has_key(DelimitedOutputter._FMTFILE):
			with open(config[DelimitedOutputter._FMTFILE]) as fmtFH:
				fieldNames = [ line.strip() for line in fmtFH]
		elif config.has_key(DelimitedOutputter._FIELD_NAMES):
			fieldNames = [field for field in config[DelimitedOutputter._FIELD_NAMES]]
		else:
			raise OutputterConfigurationException("Needs to have either"+_FMTFILE + "or " +_FIELD_NAMES)
		self.fields = [field.split(".") for field in fieldNames]
		#check for delim
		self.delim = config.get(DelimitedOutputter._DELIM,DelimitedOutputter._DEF_DELIM)
		#check for output file
		outFile = config.get(DelimitedOutputter._OUTPUT_FILE,DelimitedOutputter._STDOUT)
		if outFile == DelimitedOutputter._STDOUT:
			self.outFH = sys.stdout
		else:
			if instanceID != None:
				outFile = "%s.%s" % (instanceID,outFile)
			self.outFH = open(outFile,'w')
		#write header
		self.outFH.write(self.delim.join(fieldNames)+"\n")


	def output(self,record):
		#should I use csvwriter to do this?
		res = []
		for field in self.fields:
			val = record
			for part in field:
				try:
					val = val[part]
				except KeyError:
					dotNote = ".".join(field)
					raise KeyError("Unable to find field "+part+ " in "+ dotNote)
			res.append(str(val))
		self.outFH.write(self.delim.join(res)+"\n")

class DumpOutputter(Outputter):
	_OUTPUT_FILE="outputFile"
	_STDOUT="-"

	def loadConfigJSON(self,config,instanceID=None):
		self.initJSON(config)
		outFile = config.get(DumpOutputter._OUTPUT_FILE,DumpOutputter._STDOUT)
		if outFile == DumpOutputter._STDOUT:
			self.outFH = sys.stdout
		else:
			if instanceID != None:
				outFile = "%s.%s" % (instanceID,outFile)
			self.outFH = open(outFile,'w')

	def output(self,record):
		self.outFH.write(str(record)+"\n")

#todo: write a util function that can check for required config params, can work for mappers and outputters( and other)
#todo: write closer to release resources, probably need for mapper as well



