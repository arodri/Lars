from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from zk import zkocc
from sqlalchemy.pool import QueuePool
from query import SQLMapper

class VitessSQLMapper(SQLMapper):

	UNSHARDED = [keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)]

	def loadConfigJSON(self,config):
		
		self.vtgate = config['vtgate']
		self.keyspace = config['keyspace']
		self.tablet_type = config['tablet_type']
		self.writable = config['writable']

		# will shard based on batched parameter
		self.sharded = config['sharded']

		config['engine_url'] = ""
		SQLMapper.loadConfigJSON(self, config)

		if sharded and self.batched_parameter == None:
			raise mapper.MapperConfigurationException('%s: must provide a batched parameter if sharded' % self.name)

		config['engine_url'] = ""
		
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

	def __init_cnx_pool(self):
		self.logger.info("Opening connections")
		if 'sqlite' not in self.engineUrl:
			self.__cnx_pool = sqlalchemy.create_engine(self.engineUrl, pool_size=self.queryPoolSize)
		else:
			self.__cnx_pool = sqlalchemy.create_engine(self.engineUrl)

		
	def __exec_query(self,params,batch_id):
		qStart = time.time()
		self.logger.debug("- %s - Staring query" % batch_id)
		conn = self.__cnx_pool.connect()
		if sharded:
			cursor = conn.cursor(self.keyspace, self.tablet_type, keyspace_ids=params[self.batched_parameter], writable=self.writable)
		else:
			cursor = conn.cursor(self.keyspace, self.tablet_type, keyranges=self.UNSHARDED, writable=self.writable)
		cursor.execute(self.queryString, params)
		self.logger.debug("- %s - Query returned" % batch_id)
		qEnd = time.time()
		dur = (qEnd-qStart)*1000
		self.logger.debug("- %s - Fetching results" % batch_id)
		resultData = cursor.fetchall()
		self.logger.debug("- %s - Results fetched - %0.2f ms" % (batch_id,dur))
		cursor.close()
		conn.close()
		return [ dict(zip(r.keys(), r.values())) for r in resultData ], dur


