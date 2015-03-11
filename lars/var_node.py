import argparse, sys, time, imp
import logging, os
import json
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
	def __init__(self, data_uri, query_parallelism, target_linkages, output_file, skip_linkages, workflow_config, instanceID="0"):
		self.query_proc_pool = Pool(query_parallelism)
		self.target_linkages = target_linkages
		self.http_session = Session()
		self.output_file = output_file
		self.data_uri = data_uri

		self.instanceID = instanceID 

		self.workflow = Workflow()
		self.workflow.buildJSON(workflow_config, instanceID)

		self.skip_linkages = skip_linkages

	def getData(self, linkname, r):
		logging.debug('[GET] [%s] with %s' % (linkname, r))

		j = {}
		start = time.time()
		try:
			
			resp = self.http_session.get("%s/%s" % (self.data_uri, linkname), params=r)
			logging.debug('[GOT] [%s] %s' % (linkname, resp.status_code))
			if resp.status_code == 200:
				j = resp.json()
			j['status'] = resp.status_code

		except Exception,e:
			logging.exception(e)
			j['status'] = 'ERROR - %s' % e.value
		end = time.time()
		j['linkset_time'] = '%0.2f' % ((end-start)*1000)
		return j

	def getAllData(self, req):
		def getter(ln):
			return (ln, self.getData(ln, req['request']))
		
		start = time.time()
		data = dict(map(getter, self.target_linkages))
		#return dict( self.query_proc_pool.map(getter, self.target_linkages))
		end = time.time()

		data_ms = '%0.2f' % ((end-start)*1000)

		for (lsname, ls) in data.items():
			if 'results' in ls:
				req['linksets'][lsname] = ls['results']
			if 'query_duration' in ls:
				req['timings']['linksets:query:%s' % lsname] = ls['query_duration']
			if 'linkset_time' in ls:
				req['timings']['linksets:%s' % lsname] = ls['linkset_time']
			if 'status' in ls:
				req['status']['linksets:%s' % lsname] = ls['status']
		
		return req

	def calculateAndOutput(self):
		start = time.time()
		r = request.json
		req = {
			'request':r,
			'timings':{},
			'variables':{},
			'status':{},
			'linksets':{}
		}
		
		if not self.skip_linkages:
			logging.debug("Before data")
			logging.debug(req)
			req = self.getAllData(req)
			logging.debug("After data")
			logging.debug(req)
		else:
			logging.debug('Skipped linkages')
		
		req = self.calculateVariables(req)
		end = time.time()
		tot_ms = (end-start)*1000
		req['timings']['total'] = '%0.2f' % tot_ms
		logging.debug("After variables")
		logging.debug(req)
		
		self.output(req)
		return 'OK'

	def output(self, record):
		self.output_file.write('%s\n' % json.dumps(record, default=JSONDefault))
		#self.output_file.write('%s\n' % json.dumps(record))
		self.output_file.flush()

	def calculateVariables(self, req):
		start = time.time()
		try:
			record = req['request']
			record['linksets'] = req['linksets']
			req['variables'] = self.workflow.run(record)
			if 'timings' in req['variables']:
				req['timings']['vars'] = req['variables']['timings']
		except Exception,e:
			logging.exception(e)
			req['status']['vars'] = 'ERROR'
			req['variables'] = {}
		end = time.time()
		vars_ms = '%0.2f' % ((end-start)*1000)
		req['timings']['vars:total'] = vars_ms

		return req

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
	parser.add_argument('--skip_linkages', default=False, action='store_true', help='Skip linkage pulling step. (default %(default)s')

	args = vars(parser.parse_args())

	print args
	logging.basicConfig(level=getattr(logging, args['level']), filename=args['log'], filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

	workflow_json = json.load(args['workflow.json'][0])
	args['workflow.json'][0].close()

	server = HTTPVariableServer(args['http_port'])

	varget = VariableGenerator(args['data_uri'], args['p'], args['linkages'].split(','), args['o'], args['skip_linkages'], workflow_json, args['http_port']) 

	server.add_route('/vars', varget.calculateAndOutput)
	server.start()
