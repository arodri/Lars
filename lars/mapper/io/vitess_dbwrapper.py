import sqlalchemy
import time
import logging

from vtdb import keyrange
from vtdb import keyrange_constants
from vtdb import vtgatev2
from vtdb import vtgate_cursor
from zk import zkocc
from sqlalchemy.pool import QueuePool


from .dbwrapper import DBWrapper

UNSHARDED = [keyrange.KeyRange(keyrange_constants.NON_PARTIAL_KEYRANGE)]

class VitessDBWrapper(DBWrapper):

	def __init__(self, config):
		config['engine_url'] = ''
		DBWrapper.__init__(self, config)

		self.vtgate = config['vtgate']
		self.keyspace = config['keyspace']
		self.tablet_type = config['tablet_type']
		self.writable = config['writable']

		self.connect_timeout = config.get('connect_timeout',5)
		self.timeout = config.get('timeout',600)
		self.pool_recycle = config.get('pool_recycle',60)

		self.sharded = config['sharded']
		self.batched_parameter = config.get('batched_parameter',None)
		if self.sharded and self.batched_parameter == None:
			raise Exception('Cannot shard without a batched parameter')

		self._cnx_pool = QueuePool(self.__connect, pool_size=self.query_pool_size, recycle=self.pool_recycle, timeout=self.connect_timeout)

	def execute(self, query, params={}):
		start = time.time()
		conn = self._cnx_pool.connect()
		if self.sharded:
			cursor = conn.cursor(self.keyspace, self.tablet_type, keyspace_ids=params[self.batched_parameter], writable=self.writable)
		else:
			cursor = conn.cursor(self.keyspace, self.tablet_type, keyranges=UNSHARDED, writable=self.writable)
		cursor.begin()
		cursor.execute(query, params)
		cursor.commit()
		qtime = (time.time() - start)*1000

		start = time.time()
		keys = [ f[0] for f in cursor.description ]
		resultData = cursor.fetchall()
		cursor.close()
		conn.close()
		rtime = (time.time() - start)*1000
		result_dicts = [ dict(zip(keys, values)) for values in resultData ]

		return result_dicts, qtime, rtime

	def __connect(self):
		return vtgatev2.connect({'vt':[self.vtgate]}, self.timeout)

