import mapper
import time
import logging
import sqlalchemy
from sqlalchemy.pool import QueuePool

class SQLMapper(mapper.Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.engineUrl = config["engine_url"]
		self.queryPoolSize = config.get("query_pool_size", 2)
		self.queryLogging = config.get("query_logging", False)
		self.outputKey = config["outputKey"]
		self.queryString = config.get("queryString", None)
		self.queryFile = config.get("queryFile", None)
		self.parameters = config.get("parameters", [])
		self.__setSkipValues(config.get("skip_values",{}))
		logging.debug(config)
		if not (self.queryString == None) ^ (self.queryFile == None):
			raise mapper.MapperConfigurationException("%s: must configure 'queryFile' XOR 'queryString'" % self.name)

		self.__initalize()

	def __setSkipValues(self, skip_values):
		self.skip_values = {}
		for sv in skip_values:
			param = sv["parameter"]
			values = set(sv["values"])
			self.skip_values[param] = values

	def __initalize(self):
		self.outputKeyTiming = self.outputKey+"_QUERY_TIME"
		self.provides = [ self.outputKey, self.outputKeyTiming ]


		self.logger = logging.getLogger(self.name)
		if self.queryLogging:
			logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
		
		if self.queryString == None:
			self.logger.info("Reading query")
			with open(self.queryFile,'r') as qf:
				self.queryString = qf.read()
		self.logger.info("Testing query string subsitutions")
		self.queryString % dict(zip(self.parameters,self.parameters))

		self.logger.info("Opening connections")
		if 'sqlite' not in self.engineUrl:
			self.__cnx_pool = sqlalchemy.create_engine(self.engineUrl, pool_size=self.queryPoolSize)
		else:
			self.__cnx_pool = sqlalchemy.create_engine(self.engineUrl)

	def process(self,record):
		params = {}
		for param in self.parameters:
			if param in self.skip_values and record[param] in self.skip_values[param]:
				record[self.outputKey] = []
				record[self.outputKeyTiming] = -1
				return record
			else:
				params[param] = record[param]

		qStart = time.time()
		self.logger.debug("Staring query")
		results = self.__cnx_pool.execute(self.queryString, params)
		self.logger.debug("Query returned")
		qEnd = time.time()
		self.logger.debug("Fetching results")
		resultData = results.fetchall()
		self.logger.debug("Results fetched")
		record[self.outputKey] = [ dict(zip(r.keys(), r.values())) for r in resultData ]
		record[self.outputKeyTiming] = "%0.2f" % ((qEnd-qStart)*1000)

		return record
			
			


