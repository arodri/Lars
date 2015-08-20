import sqlalchemy
import time
import logging
from .dbwrapper import DBWrapper

class SQLAlchemyDBWrapper(DBWrapper):
	def __init__(self, config):
		DBWrapper.__init__(self, config)

		if 'sqlite' not in self.engine_url:
			self._cnx_pool = sqlalchemy.create_engine(self.engine_url)
		else:
			self._cnx_pool = sqlalchemy.create_engine(self.engine_url, pool_size=self.query_pool_size)

		if config.get('query_logging', False):
			logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

	def execute(self, query, params={}):
		qstart = time.time()
		results = self._cnx_pool.execute(query, params) 
		qtime = (time.time() - qstart)*1000

		if results.returns_rows:

			rstart = time.time()
			resultData = results.fetchall()
			rtime = (time.time() - rstart)*1000

			result_dicts = [ dict(zip(r.keys(), r.values())) for r in resultData]
		else:
			result_dicts = []
			rtime = 0.0

		return result_dicts,qtime,rtime 
