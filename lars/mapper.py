import logging
import loggingAdapters
import time
import importlib

class Mapper:

	def isValid(self):
		pass

	def process(self, record):
		try:
			raise NotImplementedError( "%s: the 'process' method is not impleted." % self.name)
		except KeyError:
			raise NotImplementedError("One of the mappers does not have the 'process' method implemented")

	def loadConfigJSON(self,config):
		self.initJSON(config)

	def initJSON(self,config):
		try:
			self.name = config["name"]
		except KeyError:
			raise MapperConfigurationException("Mapper, "+self.__class__.__name__+" does not have a name attribute")

		try:
			self.provides = config["provides"]
		except KeyError:
			pass

		try:
			self.requires = config["requires"]
		except KeyError:
			pass
		self.skip = config.get("skip",False)

	def processWrapper(self, record, timing=True, error=True):
		start = time.time()
		self.logger.setContext(record.context)
		#self.context["record"] = record
		#self.context["record_id"] = record.get_record_id()
		try:
			record = self.process(record)
		except Exception, e:
			self.logger.exception(e)
			if error:
				raise
		end = time.time()
		if timing:
			record["%s_TIMER" % self.name] = (end-start)*1000
		return record
	
	def stop(self):
		pass

	def initLogger(self):
		logger = logging.getLogger(self.name)
		self.logger = loggingAdapters.WorkflowLoggerAdapter(logger,None)


	def verifyParallizable(self):
		try:
			isList = type(self.provides) == list
			if not isList:
				raise MapperConfigurationException("%s cannot be parallelized, 'provides' attribute is not a list" % self.__class__.__name__) 
		except AttributeError:
			raise MapperConfigurationException("%s cannot be parallelized, does not have a 'provides' attribute" % self.__class__.__name__)

# Needed to pass a generic builder to a parallelized mapper so that the thread/process can instantiate the mapper on it's own.
class MapperBuilder:

	def getMapper(self):
		raise NotImplementedError("Whoops, getMapper is not implemented for this builder")

class JSONMapperBuilder(MapperBuilder):
	def __init__(self, json_config):
		self.config = json_config


	@staticmethod
	def buildFromJSON(config):
		parsed_class_path = config["class"].split(".")
		module_name,class_name = ('.'.join(parsed_class_path[:-1]), parsed_class_path[-1])
		thisMod = importlib.import_module(module_name)
		thisMapClass = getattr(thisMod, class_name)
		thisMap = thisMapClass()
		thisMap.initJSON(config)
		skip = thisMap.skip
		if not skip:
			thisMap.loadConfigJSON(config)
			thisMap.isValid()
			thisMap.initLogger()
	#	else:
	#		thisMap = None
		return (thisMap,skip)
	
	def getMapper(self):
		return self.buildFromJSON(self.config)


class MapperConfigurationException(Exception):
	pass



