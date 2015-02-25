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




class MapperConfigurationException(Exception):
	pass
