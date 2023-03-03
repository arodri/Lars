#!/usr/bin/env python
import argparse, sys, time, re
import logging, os
import json
import threading
from threading import Timer
from threading import RLock

import tornado
from tornado.web import RequestHandler, Application, url
from tornado.ioloop import IOLoop
import tornado.wsgi

import lars
from lars.workflow import Workflow
from lars.workflow import replace_env
from lars.util import PeriodicTask

def wsgi(default_workflow):
    return wsgimulti([('default', default_workflow)])
 
def wsgimulti(workflows):
    loglevel = os.environ.get("LOGLEVEL", "INFO")
    lars.log.configure_json_stderr(loglevel)
    manager = NewWorkflowManager(workflows, 0)
    server = HTTPWorkflowServer(0, manager)
    return server.wsgi()



def JSONDefault(obj):
	import datetime
	if isinstance(obj, datetime.datetime):
		return obj.strftime('%Y%m%d_%H:%M:%S.%f')


class WorkflowWrapper:

	def __init__(self, name, config, serviceID):
		self.name = name
		self._workflow = Workflow()
		self._workflow.buildJSON(config, instanceID="%s.%s" % (serviceID, name))

		self.st_lock = RLock()

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

		self.st_lock.acquire()
		self.updateCounts((end-start)*1000)
		self.st_lock.release()
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
		return dict(list(self.tps.items()) + [('total_served',self.total_served),('resp_100',self.response_100),('resp_1000',self.response_1000),('resp_10000',self.response_10000)])



class WorkflowManager:
	def __init__(self, wokflows=[]):
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
		return { 'workflows':list(self._workflows.keys()), 'stats': dict([ (w.name, w.getStats()) for w in list(self._workflows.values()) ]) }

	def workflowSummary(self,name):
		return self._workflows[name].getStats()

def NewWorkflowManager(workflows, port):
    names = set([])
    manager = WorkflowManager()
    name_re_str = r'^([a-zA-Z0-9-_]+)$'
    name_re = re.compile(name_re_str)
    for (name, wfpath) in workflows:
        if not name_re.match(name):
            raise Exception("invalid name format: %s, must be: %s" % (name, name_re_str))
        if name in names:
            raise Exception("names must be unique: %s" % name)
        if not os.path.isfile(wfpath):
            raise Exception("unable to find wokflow: %s" % wfpath)
        names.add(name)

    for (name, wfpath) in workflows:
        with open(wfpath, 'r') as wf_fh:
            config_str = replace_env(wf_fh.read())
            config = json.loads(config_str)
            manager.add(name, config, port)
    return manager

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
				self.write(json.dumps(self.workflow_manager.process(name, record), default=JSONDefault))
			except ValueError:
				self.send_error(400, reason="Unable to parse JSON record")
		else:
			self.send_error(400, reason="Workflow '%s' does not exist" % name)

class HeartBeatHandler(tornado.web.RequestHandler):
	def get(self):
		self.write('OK')

class HTTPWorkflowServer(object):
	def __init__(self, port, wf_manager):
		self.logger = logging.getLogger('lars.HTTPWorkflowServer')
		self.logger.info('Configuring web server')
		
		self.wf_manager = wf_manager
		self.app = Application([
			url(r"/$", HeartBeatHandler),
			url(r"/lars", WorkflowHandler, dict(workflow_manager=self.wf_manager, serviceID=port)),
			url(r"/lars/([a-zA-Z0-9-_]+)$", WorkflowHandler, dict(workflow_manager=self.wf_manager, serviceID=port))
		])
		self.port = port

	def start(self):
		self.app.listen(self.port)
		IOLoop.current().start()

	def wsgi(self):
		return tornado.wsgi.WSGIAdapter(self.app)


if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="A HTTP-exposed workflow server.")

	#parser.add_argument('workflow.json', nargs=1, type=argparse.FileType('r'), help='JSON workflow configuration.')
	# options
	parser.add_argument('--log', metavar='FILE', default=None, help='Log file. (default: STDOUT)')
	parser.add_argument('--loglevel', metavar='LEVEL', choices=["INFO","DEBUG","WARNING","ERROR"], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	parser.add_argument('--http_port', metavar='PORT', default=9000, type=int, help='HTTP port to listen on. (default: %(default)s')
	parser.add_argument('--default_workflow', metavar='JSON_FILE', default=None, type=str, help='Workflow to put on lars/default. Useful for running on commandline')
	parser.add_argument('-w', '--workflow', metavar=('NAME', 'FILE_PATH'), default=[], action='append', nargs=2, help='Supply a pair of NAME and workflow FILE_PATH. Example: -w test-v1 ./workflow.json (will spin up ./workflow.json at /lars/test-v1)')

	args = parser.parse_args()

	if args.log != None:
		lars.log.configure_json_file(args.log, level=args.loglevel)
	else:
		lars.log.configure_json_stderr(args.loglevel)

		names = set([])
		workflows = args.workflow
		if args.default_workflow != None:
			workflows = [('default', args.default_workflow)] + workflows
		try:
			manager = NewWorkflowManager(workflows, args.http_port)
		except Exception as e:
			print(("error: %s" % e))
			sys.exit(1)
	server = HTTPWorkflowServer(args.http_port, manager)
	server.start()
