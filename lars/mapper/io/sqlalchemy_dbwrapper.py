import sqlalchemy
import time
import logging
from .dbwrapper import DBWrapper
from lars.util import PeriodicTask

class SQLAlchemyDBWrapper(DBWrapper):
	def __init__(self, config):
		DBWrapper.__init__(self, config)

		if 'sqlite' not in self.engine_url:
			self._cnx_pool = sqlalchemy.create_engine(self.engine_url,pool_recycle=3600)
		else:
			self._cnx_pool = sqlalchemy.create_engine(self.engine_url, pool_size=self.query_pool_size)

		if config.get('query_logging', False):
			logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
                self.max_attempts = config.get("max_attempts",3)
                self.initial_delay = config.get("initial_delay",.05)
                self.dummy_query = config.get("dummy_query","SELECT 'I AM A DUMMY'")
                ping = config.get("ping_delay",30)
                #check to see if connection params are good
                t = PeriodicTask(ping, self.arod_query,jitter=.25)
                t.run()
                try:
                    self.arod_query()
                except sqlalchemy.exc.OperationalError:
                    raise Exception("Failed to connect using url %s" % self.engine_url)

        def arod_query(self):
            logger = logging.getLogger()
            logger.info("connection heartbeat for DB")
            self._cnx_pool.execute(self.dummy_query)

        def retry_execs(self,query,params):
            attempts = 1
            delay = self.initial_delay
            while attempts < self.max_attempts:
                try:
                    return self._cnx_pool.execute(query,params)
                except sqlalchemy.exc.DBAPIError,e:
                    attempts += 1
                    log = logging.getLogger()
                    logging.warning("DB Connection Failed: waiting %s seconds then retrying" % delay)
                    time.sleep(delay)
                    delay *= 2
            return self._cnx_pool.execute(query,params)        


	def execute(self, query, params={}):
		qstart = time.time()
		results = self.retry_execs(query,params)
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
