import logging
import time
import importlib

class Mapper:

	def isValid(self):
		pass

	def run(self, record):
		raise NotImplementedError( "Should have implemented this")

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

	def runWrapper(self, record, timing=True, error=True):
		start = time.time()
		try:
			record = self.run(record)
		except Exception, e:
			self.logger.exception(e)
			if error:
				raise
		end = time.time()
		if timing:
			record["%s_TIMER" % self.name] = (end-start)*1000
		return record

	def initLogger(self):
		self.logger = logging.getLogger(self.name)

	def verifyParallizable(self):
		try:
			isList = type(self.provides) == list
			if not isList:
				raise MapperConfigurationException("%s cannot be parallelized, 'provides' attribute is not a list" % self.__class__.__name__) 
		except AttributeError:
			raise MapperConfigurationException("%s cannot be parallelized, does not have a 'provides' attribute" % self.__class__.__name__)

def buildFromJSON(config):
	parsed_class_path = config["class"].split(".")
	module_name,class_name = ('.'.join(parsed_class_path[:-1]), parsed_class_path[-1])
	thisMod = importlib.import_module(module_name)
	thisMapClass = getattr(thisMod, class_name)
	thisMap = thisMapClass()
	thisMap.initJSON(config)
	thisMap.loadConfigJSON(config)
	thisMap.isValid()
	thisMap.initLogger()

	return thisMap


class MapperConfigurationException(Exception):
	pass
