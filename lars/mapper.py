import logging
import time

class Mapper:

	def isValid(self,mapper):
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
 	

class MapperConfigurationException(Exception):
	pass
