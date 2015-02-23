import argparse, sys, time, imp
import logging, os, json
import subprocess, requests
from requests import Session

from multiprocessing import Pool
import sqlalchemy
from sqlalchemy.pool import QueuePool

from flask.views import View
from flask import Flask
from flask import request
from flask.views import MethodView

from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop

#from gevent.wsgi import WSGIServer

from workflow import Workflow

def JSONDefault(obj):
	import datetime
	if isinstance(obj, datetime.datetime):
		return obj.strftime('%Y%m%d_%H:%M:%S.%f')

class VariableGenerator(object):
	def __init__(self, data_uri, query_parallelism, target_linkages, output_file, workflow_config):
		self.query_proc_pool = Pool(query_parallelism)
		self.target_linkages = target_linkages
		self.http_session = Session()
		self.output_file = output_file
		self.data_uri = data_uri

		self.workflow = Workflow()
		self.workflow.buildJSON(workflow_config)

	def getData(self, linkname, r):
		logging.debug('[GET] [%s] with %s' % (linkname, r))

		j = {}
		try:
			resp = self.http_session.get("%s/%s" % (self.data_uri, linkname), params=r)
			logging.debug('[GOT] [%s] %s' % (linkname, resp.status_code))
			if resp.status_code == 200:
				j = resp.json()
			j['status'] = resp.status_code

		except Exception,e:
			logging.exception(e)
			j['status'] = 'ERROR - %s' % e.value
		return j

	def getAllData(self, r):
		def getter(ln):
			return (ln, self.getData(ln, r))
		
		#return dict( self.query_proc_pool.map(getter, self.target_linkages))
		start = time.time()
		data = dict(map(getter, self.target_linkages))
		end = time.time()

		data_ms = '%0.2f' % ((end-start)*1000)
	
		record = {
			'request':r,
			'timings':{},
			'linksets':{},
			'status':{}
		}

		for (lsname, ls) in data.items():
			if 'results' in ls:
				record['linksets'][lsname] = ls['results']
			if 'query_duration' in ls:
				record['timings']['linksets:%s' % lsname] = ls['query_duration']
			if 'status' in ls:
				record['status']['linksets:%s' % lsname] = ls['status']
		
		return record

	def calculateAndOutput(self):
		r = request.json
		logging.debug(r)
		record = self.getAllData(r)
		logging.debug(record)
		record = self.calculateVariables(record)
		record['timings']['total'] = '%0.2f' % sum([ float(ms) for ms in record['timings'].values()])

		logging.debug(record)
		self.output(record)
		return 'OK'

	def output(self, record):
		self.output_file.write('%s\n' % json.dumps(record, default=JSONDefault))
		self.output_file.flush()

	def calculateVariables(self, record):
		start = time.time()
		try:
			record = self.workflow.run(record)
		except Exception,e:
			logging.exception(e)
			record['status']['vars'] = 'ERROR'
		end = time.time()
		vars_ms = '%0.2f' % ((end-start)*1000)
		record['timings']['vars'] = vars_ms

		return record

class HTTPVariableServer(object):
	def __init__(self, port):
		logging.info('Configuring web server')
		self.app = Flask(__name__)
		self.port=port
		self.app.add_url_rule("/", view_func=lambda: "OK")

	# function can use flask.request to get at the original request
	def add_route(self, uri, function):
		logging.debug("Adding uri: %s" % (uri))

		class API(MethodView):
			def get(self):
				return self.post()
			def post(self):
				print "In Post!"
				print request
				return function()

		self.app.add_url_rule(uri, view_func=API.as_view(uri))

	def start(self):
		# Gevent - 120 < x < 400
		#self.http_server = WSGIServer(('',self.port), self.app)
		#self.http_server.serve_forever()

		# Tornado - 400tps single-threaded
		self.http_server = HTTPServer(WSGIContainer(self.app))
		self.http_server.listen(self.port)
		IOLoop.instance().start()
		
		# Flask - 120 tps single-threaded
		#self.app.run(host='127.0.0.1', port=self.port, debug=True, threaded=True)

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="A REST-exposed database querying api.")

	parser.add_argument('workflow.json', nargs=1, type=argparse.FileType('r'), help='JSON workflow configuration.')
	# required paramaters
	parser.add_argument('--data_uri', metavar='HOST', default='127.0.0.1:8080/api/0.1', help='Data service to query')
	parser.add_argument('-p', metavar='PARALLELISM', default=2, type=int, help='Query parallelism to use on each request. (default: %(default)s)')
	parser.add_argument('-o', metavar='FILE', default=sys.stdout, type=argparse.FileType('w'), help='Output file. (default: %(default)s)')
	# options
	parser.add_argument('--log', metavar='FILE', default="server.log", help='Log file. (default: %(default)s)')
	#parser.add_argument('--log', metavar='FILE', type=argparse.FileType('a'), default="server.log", help='Log file. (default: %(default)s)')
	parser.add_argument('--level', metavar='LEVEL', choices=["INFO","DEBUG"], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	parser.add_argument('--http_port', metavar='PORT', default=9000, type=int, help='HTTP port to listen on. (default: %(default)s')
	
	parser.add_argument('--linkages',  metavar="LINKAGES", default="simpletest", help='CSV list of linksets to query the data api. (default: %(default)s)')

	args = vars(parser.parse_args())

	print args
	logging.basicConfig(level=getattr(logging, args['level']), filename=args['log'], filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

	workflow_json = json.load(args['workflow.json'][0])
	args['workflow.json'][0].close()

	server = HTTPVariableServer(args['http_port'])

	varget = VariableGenerator(args['data_uri'], args['p'], args['linkages'].split(','), args['o'], workflow_json) 

	server.add_route('/vars', varget.calculateAndOutput)
	server.start()
