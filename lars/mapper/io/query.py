import mapper
import time
import logging
import math
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
		self.batched_parameter = config.get("batched_parameter", None)
		self.batch_size = float(config.get("batch_size",10))
		self.__setSkipValues(config.get("skip_values",{}))
		logging.debug(config)
		if not (self.queryString == None) ^ (self.queryFile == None):
			raise mapper.MapperConfigurationException("%s: must configure 'queryFile' XOR 'queryString'" % self.name)

		self.__initalize()
		self.__init_cnx_pool()

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
		p_dict = dict(zip(self.parameters,self.parameters))
		if self.batched_parameter != None:
			p_dict[self.batched_parameter] = self.batched_parameter
		self.queryString % p_dict

	def __init_cnx_pool(self):
		self.logger.info("Opening connections")
		if 'sqlite' not in self.engineUrl:
			self.__cnx_pool = sqlalchemy.create_engine(self.engineUrl, pool_size=self.queryPoolSize)
		else:
			self.__cnx_pool = sqlalchemy.create_engine(self.engineUrl)

	def process(self,record):
		params = {}
		results = []
		q_time = 0
		record[self.outputKey] = None
		record[self.outputKeyTiming] = -1
		# check for empty parameter values
		for param in self.parameters:
			if param in self.skip_values and record[param] in self.skip_values[param]:
				return record
			else:
				params[param] = record[param]

		if self.batched_parameter == None:
			(r,dur) = self.__exec_query(params,0)
			q_time += dur
			results += r
		else:
			values = filter(lambda v: not (self.batched_parameter not in self.skip_values and v in self.skip_values[self.batched_parameter]), record[self.batched_parameter])
			self.logger.debug(values)
			if len(values) == 0:
				return record
			num_batches = math.ceil(len(values)/self.batch_size)
			this_batch_size = int(math.ceil(len(values)/num_batches))
			self.logger.debug("Querying %s elements in %s batches of %s records" % (len(values), int(num_batches), this_batch_size))
			for i in range(int(num_batches)):
				lower = i*this_batch_size
				upper = (i+1)*this_batch_size
				params[self.batched_parameter] = values[lower:upper]
				(r,dur) = self.__exec_query(params,i)
				q_time += dur
				results += r
		record[self.outputKey] = results
		record[self.outputKeyTiming] = q_time

		return record
		
	def __exec_query(self,params,batch_id):
		qStart = time.time()
		self.logger.debug("- %s - Staring query" % batch_id)
		results = self.__cnx_pool.execute(self.queryString, params)
		self.logger.debug("- %s - Query returned" % batch_id)
		qEnd = time.time()
		dur = (qEnd-qStart)*1000
		self.logger.debug("- %s - Fetching results" % batch_id)
		resultData = results.fetchall()
		self.logger.debug("- %s - Results fetched - %0.2f ms" % (batch_id,dur))
		return [ dict(zip(r.keys(), r.values())) for r in resultData ], dur


