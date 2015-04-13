#!/usr/bin/env python
import argparse, sys, time
import logging, os
import json
import threading
from threading import Timer
#import subprocess, requests
#from requests import Session

#from flask.views import View
#from flask import Flask
#from flask import request
#from flask.views import MethodView

import tornado
from tornado.web import RequestHandler, Application, url
from tornado.ioloop import IOLoop

from workflow import Workflow

def JSONDefault(obj):
	import datetime
	if isinstance(obj, datetime.datetime):
		return obj.strftime('%Y%m%d_%H:%M:%S.%f')

class PeriodicTask(object):
	def __init__(self, interval, callback, daemon=True, **kwargs):
		self.interval = interval
		self.callback = callback
		self.daemon   = daemon
		self.kwargs   = kwargs

	def run(self):
		self.callback(**self.kwargs)
		t = Timer(self.interval, self.run)
		t.daemon = self.daemon
		t.start()

class WorkflowWrapper:

	def __init__(self, name, config, serviceID):
		self.name = name
		self._workflow = Workflow()
		self._workflow.buildJSON(config, instanceID="%s.%s" % (serviceID, name))

		self.total_served = 0

		self.response_100 = 0
		self.response_1000 = 0
		self.response_10000 = 0

		self.tps = {}
		self.tps_prev = {}

		self.timer(10)
		self.timer(30)
		self.timer(60)

	def updateTPS(self, dur):
		key = 'tps_%s' % dur
		t = time.time()
		(prev_cnt, prev_time) = self.tps_prev[key]
		if prev_time != t:
			self.tps[key] = (self.total_served - prev_cnt)/(t-prev_time)

	def timer(self,dur):
		self.tps_prev['tps_%s' % dur] = (self.total_served, time.time())
		t = PeriodicTask(dur, self.updateTPS, dur=dur)
		t.run()

	def stop(self):
		self._workflow.stop()

	def process(self, record):

		start = time.time()
		resp = self._workflow.process(record)
		end = time.time()

		self.updateCounts((end-start)*1000)
		return resp
		
	def updateCounts(self, ms):
		if self.response_100 == 0:
			self.response_100 = ms
		else:
			self.response_100  =  ms * .01   + self.response_100   * .99

		if self.response_1000 == 0:
			self.response_1000 = ms
		else:
			self.response_1000 =  ms * .001  + self.response_1000  * .999

		if self.response_10000 == 0:
			self.response_10000 = ms
		else:
			self.response_10000 = ms * .0001 + self.response_10000 * .9999
		
		self.total_served += 1

	def getStats(self):
		return dict(self.tps.items() + [('total_served',self.total_served),('resp_100',self.response_100),('resp_1000',self.response_1000),('resp_10000',self.response_10000)])



class WorkflowManager:
	def __init__(self):
		self._workflows = {}
	
	def exists(self, name):
		return name in self._workflows

	def add(self, name, config, serviceID):
		self._workflows[name] = WorkflowWrapper(name,config,serviceID)
		
	def remove(self, name):
		if name in self._workflows:
			self._workflows[name].stop()
			del(self._workflows[name])

	def process(self, name, data):
		return self._workflows[name].process(data)

	def summary(self):
		return { 'workflows':self._workflows.keys(), 'stats': dict([ (w.name, w.getStats()) for w in self._workflows.values() ]) }

	def workflowSummary(self,name):
		return self._workflows[name].getStats()


class WorkflowHandler(tornado.web.RequestHandler):
	
	def initialize(self, workflow_manager, serviceID):
		self.workflow_manager = workflow_manager
		self.serviceID = serviceID

	# get the workflow config
	def get(self, name=None):
		if name == None:
			self.write(self.workflow_manager.summary())
		elif not self.workflow_manager.exists(name):
			self.send_error(400, reason="Unknown workflow '%s'" % name)
			return
		else:
			self.write(self.workflow_manager.workflowSummary(name))

	# add a workflow
	def put(self, name):
		if not self.workflow_manager.exists(name):
			try:
				config = json.loads(self.request.body)
				self.workflow_manager.add(name, config, self.serviceID)
				self.set_status(201)
			except ValueError:
				self.send_error(400, reason="Unable to parse JSON workflow config")
		else:
			self.send_error(400, reason="Workflow '%s' already exists" % name)

	# remote a workflow
	def delete(self, name):
		if self.workflow_manager.exists(name):
			self.workflow_manager.remove(name)
		else:
			self.send_error(400, reason="Unknown workflow '%s'" % name)
		
	def post(self, name):
		if self.workflow_manager.exists(name):
			try:
				record = json.loads(self.request.body)
			except ValueError:
				self.send_error(400, reason="Unable to parse JSON record")
			self.write(json.dumps(self.workflow_manager.process(name, record), default=JSONDefault))
		else:
			self.send_error(400, reason="Workflow '%s' does not exist" % name)

class HeartBeatHandler(tornado.web.RequestHandler):
	def get(self):
		self.write('OK')

class HTTPWorkflowServer(object):
	def __init__(self, port, workflow=None):
		self.logger = logging.getLogger('HTTPWorkflowServer')
		self.logger.info('Configuring web server')
		
		self.wf_manager = WorkflowManager()
		if workflow != None:
			self.wf_manager.add('default',workflow,port)

		self.app = Application([
			url(r"/$", HeartBeatHandler),
			url(r"/lars", WorkflowHandler, dict(workflow_manager=self.wf_manager, serviceID=port)),
			url(r"/lars/([a-zA-Z0-9]+)$", WorkflowHandler, dict(workflow_manager=self.wf_manager, serviceID=port))
		])
		self.app.listen(port)

	def start(self):
		IOLoop.current().start()

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="A HTTP-exposed workflow server.")

	#parser.add_argument('workflow.json', nargs=1, type=argparse.FileType('r'), help='JSON workflow configuration.')
	# options
	parser.add_argument('--log', metavar='FILE', default=None, help='Log file. (default: STDOUT)')
	#parser.add_argument('--log', metavar='FILE', type=argparse.FileType('a'), default="server.log", help='Log file. (default: %(default)s)')
	parser.add_argument('--loglevel', metavar='LEVEL', choices=["INFO","DEBUG","WARNING","ERROR"], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	parser.add_argument('--http_port', metavar='PORT', default=9000, type=int, help='HTTP port to listen on. (default: %(default)s')
	parser.add_argument('--default_workflow', metavar='JSON_FILE', default=None, type=argparse.FileType('r'), help='Workflow to put on lars/default. Useful for running on commandline')
	
	args = parser.parse_args()

	logging_config = dict(level=getattr(logging, args.loglevel), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	if args.log != None:
		logging_config['filename'] = args.log
		logging_config['filemode'] = 'a'
	
	workflow_config = None
	if args.default_workflow != None:
		workflow_config = json.loads(args.default_workflow.read())

	logging.basicConfig(**logging_config)

	#workflow_json = json.load(args.workflow.json[0])
	#args,workflow.json[0].close()

	server = HTTPWorkflowServer(args.http_port, workflow_config)
	server.start()
