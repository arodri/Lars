import argparse,sys,logging,os
import json,subprocess,time
from multiprocessing import Pool

import sqlalchemy
from sqlalchemy.pool import QueuePool

from flask import Flask
from flask import request
from flask.views import View

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

#from gevent.wsgi import WSGIServer

class HTTPDataServer(object):
	"""
	@param dict db_config 		connection configuration dictionary
	@param dict query_config 	query configuration dictionary
	"""
	def __init__(self, port, db_config, query_config):
		logging.info('Configuring web server')

		self.app = Flask(__name__)
		self.num_queries = 0
		for uri in query_config['apis']:
			for query in uri['queries']:
				self.num_queries += 1
		
		logging.info('Loading %s queries' % self.num_queries)

		for uri in query_config['apis']:
			for query in uri['queries']:
				self._add_route(uri['uri'], query['name'], query['query'], query['params'])
		self.app.add_url_rule("/", view_func=lambda: "OK")

		logging.info('Opening connections')
		if 'sqlite' not in db_config['url']:
			self._cnx_pool = sqlalchemy.create_engine(db_config['url'], pool_size=db_config['pool_size'], echo=True)
		else:
			self._cnx_pool = sqlalchemy.create_engine(db_config['url'], echo=True)
		self.port=port

	def _add_route(self, uri, name, query, param_template):
		logging.debug("Adding %s/%s, params=%s, query=%s" % (uri, name, param_template, query))
		self.app.add_url_rule("%s/%s" % (uri, name), view_func=self._make_handler(name, query, param_template), endpoint=name)
		self.app.add_url_rule("%s/%s/info" % (uri, name), view_func=lambda: str(param_template), endpoint="%s/%s" % (name,"info"))

	def _make_handler(self, name, query, param_template):
		# sqlalchemy.exc.ProgrammingError (BAD QUERY)
		# KeyError (missing key)
		# NameError (bad query/params, field DNE
		def handler():
			start = time.time()
			results = self._cnx_pool.execute(query, request.args)
			end = time.time()
			ms = ((end-start)*1000)
			result_dicts = [ dict(zip(r.keys(), r.values())) for r in results.fetchall() ]
			
			resp = {
				'name':name,
				'query_duration':'%0.2f' % ms,
				'results': result_dicts
			}
			if ('debug' in request.args):
				resp['avail_params'] = param_template
				resp['query'] = query
				resp['params'] = request.args
			return json.dumps(resp)
		return handler

	def start(self):
	
		# Gevent - 120 < x < 400
		#self.http_server = WSGIServer(('',self.port), self.app)
		#self.http_server.serve_forever()

		# Tornado - 600tps single-threaded
		self.http_server = HTTPServer(WSGIContainer(self.app))
		self.http_server.listen(self.port)
		IOLoop.instance().start()
		
		# Flask - 120 tps single-threaded
		#self.app.run(host='127.0.0.1', port=self.port, debug=True, threaded=True)

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="A REST-exposed database querying api.")
	# required paramaters
	parser.add_argument('db_config', metavar='database_config.json', type=argparse.FileType('r'), nargs=1, help='Connection configuration file.')
	parser.add_argument('query_config', metavar='query_config.json', type=argparse.FileType('r'), nargs=1, help='Query configuration file.')
	
	# options
	parser.add_argument('--log', metavar='FILE', default="server.log", help='Log file. (default: %(default)s)')
	#parser.add_argument('--log', metavar='FILE', type=argparse.FileType('a'), default="server.log", help='Log file. (default: %(default)s)')
	parser.add_argument('--level', metavar='LEVEL', choices=["INFO","DEBUG"], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	parser.add_argument('--http_port', metavar='PORT', default=9000, type=int, help='HTTP port to listen on. (default: %(default)s')
	args = vars(parser.parse_args())

	print args
	logging.basicConfig(level=getattr(logging, args['level']), filename=args['log'], filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	print args['db_config'][0]
	print args['query_config'][0]

	db_config = json.loads(args['db_config'][0].read())
	args['db_config'][0].close()

	query_config = json.loads(args['query_config'][0].read())
	args['query_config'][0].close()

	server = HTTPDataServer(args['http_port'], db_config, query_config)
	server.start()
