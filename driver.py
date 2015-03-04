#!/usr/bin/env python
import signal,sys
from subprocess import Popen
from multiprocessing import Pool
import shlex
import argparse
import logging
import time
import os
from jinja2 import Template
from lars.feeder import Feeder
import requests


class Driver(object):

	def __init__(self, 
		queries_json,
		connection_json,
		workflow_json,
		haproxy_stat_port, 
		haproxy_data_port, data_ports, 
		haproxy_var_port, var_ports,
		skip_linkages,
		query_parallelism, linkages, 
		loglevel,
		input_file, delim, num_feeders):

		self.datanodes = []
		self.haproxies = []
		self.varnodes = []
		self.feeders = []

		self.queries_json = queries_json
		self.connection_json = connection_json
		self.workflow_json = workflow_json

		self.haproxy_stat_port = haproxy_stat_port
		
		self.data_ports = data_ports
		self.haproxy_data_port = haproxy_data_port
		
		self.var_ports = var_ports
		self.haproxy_var_port = haproxy_var_port
		self.query_parallelism = query_parallelism
		self.linkages = linkages

		self.loglevel = loglevel

		self.skip_linkages = skip_linkages

		self.input_file = input_file
		self.delim = delim
		self.num_feeders = num_feeders

		self.logger = logging.getLogger('Driver')

	@staticmethod
	def stop_process(process):
		try:
			process.terminate()
			process.wait()
		except OSError,e:
			pass

	@staticmethod
	def stop_processes(processes):
		if len(processes) > 0:
			#p = Pool(len(processes))
			map(Driver.stop_process, processes)

	def stop_all(self):

		self.logger.info("Stopping feeder nodes...")
		Driver.stop_processes(self.feeders)
		self.logger.info("all feeders stopped")

		self.logger.info("Stopping data nodes...")
		Driver.stop_processes(self.datanodes)
		self.logger.info("all data nodes stopped")

		self.logger.info("Stopping var nodes...")
		Driver.stop_processes(self.varnodes)
		self.logger.info("all var nodes stopped")

		self.logger.info("Stopping haproxy...")
		Driver.stop_processes(self.haproxies)
		self.logger.info("haproxy stopped")

		
		self.logger.info("Congratulations, all services stopped. Farewell!")

	def make_signal_handler(self, exit_code):
		def handler(signal, frame):
			self.logger.warn("received signal %s" % signal)
			self.stop_all()
			sys.exit(exit_code)
		return handler

	def start_feeder(self, start_byte, end_byte):
		log_file_str = "%s.%s_%s" % (os.path.basename(self.input_file), start_byte, end_byte)
		with open('logs/feeder.%s.out' % log_file_str, 'ab') as out, open('logs/feeder.%s.err' % log_file_str, 'ab') as err:
			cmd_str = """python lars/feeder.py
				--var_uri=%(var_uri)s
				--log=%(feeder_log)s
				--level=%(loglevel)s
				--start_byte=%(start_byte)s
				--end_byte=%(end_byte)s
				%(input_file)s %(delim)s""".replace('\n', ' ').replace('\t', '')
			cmd_str = cmd_str % {
				"var_uri":"http://localhost:%s/vars" % self.haproxy_var_port,
				"feeder_log":"logs/feeder.%s.log" % log_file_str,
				"loglevel":self.loglevel,
				"start_byte":start_byte,
				"end_byte":end_byte,
				"input_file":self.input_file,
				"delim":self.delim
			}
			self.logger.debug(cmd_str)
			cmd = shlex.split(cmd_str)
			return Popen(cmd, stdout=out, stderr=err)

	def feeders_done(self):
		# Check to see if any of the feeders are still running
		#  Popen.poll() returns None for a running process.
		is_done = []
		for feeder in self.feeders:
			try:
				is_done.append(feeder.poll() != None)
			except OSError,e:
				is_done.append(True)
		return all(is_done)

	def start_feeders(self):
		with open(self.input_file,'r') as fin:
			chunks = Feeder.getOffsets(fin, self.num_feeders)
		for (start_byte, end_byte) in chunks:
			feeder = self.start_feeder(start_byte, end_byte)
			self.feeders.append(feeder)

	def start_datanode(self, port, loglevel, queries_config, connection_config):
		with open('logs/data.node.%s.out' % port, 'ab') as out, open('logs/data.node.%s.err' % port, 'ab') as err:
			cmd_str = """python lars/data_node.py
				--http_port=%(port)s
				--log=logs/data.node.%(port)s.log
				--level=%(loglevel)s
				%(connection)s %(queries)s""".replace('\n', ' ').replace('\t', '')

			cmd_str = cmd_str % {
				'port':port,
				'loglevel':loglevel,
				'connection':connection_config,
				'queries':queries_config
			}
			self.logger.debug(cmd_str)
			cmd = shlex.split(cmd_str)
			return Popen(cmd, stdout=out, stderr=err)

	def start_haproxy(self, haproxy_port, data_proxy_port, var_proxy_port, data_ports, var_ports):
		data_nodes = zip([ "data%s" % port for port in data_ports ], data_ports)
		var_nodes = zip(["var%s" % port for port in var_ports ], var_ports)
		with open('config/haproxy.jinja2.cfg', 'r') as cfg_template:
			with open('logs/haproxy.cfg', 'w') as cfg:
				template = Template(cfg_template.read())
				cfg.write(template.render(stats_port=haproxy_port, data_proxy_port=data_proxy_port, var_proxy_port=var_proxy_port, data_nodes=data_nodes, var_nodes=var_nodes))
		cmd_str = "haproxy -f logs/haproxy.cfg"
		self.logger.debug(cmd_str)
		cmd = shlex.split(cmd_str)
		with open('logs/haproxy.out','ab') as out, open('logs/haproxy.err', 'ab') as err:
			return Popen(cmd, stdout=out, stderr=err)
	
	def start_varnode(self, port, haproxy_data_port, query_parallelism, linkages, loglevel, workflow_config, skip_linkages):
		with open('logs/var.node.%s.out' % port, 'ab') as out, open('logs/var.node.%s.err' % port, 'ab') as err:
			cmd_str = ("""python lars/var_node.py 
				--http_port=%(port)s 
				--log=logs/var.node.%(port)s.log 
				-o output/var.node.%(port)s.output
				--level=%(loglevel)s
				-p %(query_parallelism)s
				--data_uri=http://127.0.0.1:%(haproxy_data_port)s/api/0.1
				""" + ("--skip_linkages" if skip_linkages else "--linkages=%(linkages)s") + """ 
				%(workflow)s""").replace('\n', ' ').replace('\t','')
			cmd_str = cmd_str % {
				'port':port, 
				'haproxy_data_port':haproxy_data_port,
				'linkages':linkages,
				'query_parallelism':query_parallelism,
				'loglevel':loglevel,
				'workflow':workflow_config
				}
			self.logger.debug(cmd_str)
			cmd = shlex.split(cmd_str)
			return Popen(cmd, stdout=out, stderr=err)

	def run_loop(self):
		self.logger.debug("Entering run loop")
		if self.input_file != None:
			while not self.feeders_done():
				time.sleep(.25)
			self.logger.info("All done!")
			self.stop_all()
		else:
			signal.pause()
			self.stop_all()

	@staticmethod
	def wait_for_startup(url, session):
		while session.get(url).status_code != 200:
			time.sleep(.25)

	def wait_for_all_startup(self):
		s = requests.Session()
		self.logger.debug("Waiting for HAProxy to start...")
		Driver.wait_for_startup("http://localhost:%s/" % self.haproxy_stat_port, s)
		if not self.skip_linkages:
			self.logger.debug("Waiting for data nodes to start...")
			Driver.wait_for_startup("http://localhost:%s/" % self.haproxy_data_port, s)
		self.logger.debug("Waiting for var nodes to start...")
		Driver.wait_for_startup("http://localhost:%s/" % self.haproxy_var_port, s)
		s.close()

	def start_all(self):
		signal.signal(signal.SIGINT, self.make_signal_handler(0))
		
		try:
			# Start data nodes
			self.logger.info("Starting data nodes: %s" % self.data_ports)
			self.datanodes = [ self.start_datanode(port, self.loglevel, self.queries_json, self.connection_json) for port in self.data_ports ]
			self.logger.info("data nodes started")

			# Start HAProxy
			self.logger.info("Starting HAProxy. Ports: stats:%s, var:%s, data:%s" % (self.haproxy_stat_port, self.haproxy_var_port, self.haproxy_data_port))
			self.haproxies =   [ self.start_haproxy(self.haproxy_stat_port, self.haproxy_data_port, self.haproxy_var_port, self.data_ports, self.var_ports) ] 
			self.logger.info("haproxy started")

			# Start variable nodes
			self.logger.info("Starting variable nodes: %s" % self.var_ports)
			self.varnodes =  [ self.start_varnode(port, self.haproxy_data_port, self.query_parallelism, self.linkages, self.loglevel, self.workflow_json, self.skip_linkages) for port in self.var_ports ]
			self.logger.info("variable nodes started")

			# Sleep and start feeders
			if self.input_file != None:
				self.logger.info("Waiting for all services to be started...")
				self.wait_for_all_startup()
				self.logger.info("All services started, here we go!")
				self.start_feeders()
				self.logger.info("feeders started")
			self.run_loop()

		except Exception,e:
			self.logger.error("Oh no, something went terribly wrong! Check the logs!")
			self.logger.exception(e)
			self.stop_all()
			sys.exit(1)

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="Driver program for Lars")

	parser.add_argument('queries.json', nargs=1, help='JSON query configuration file for data nodes.')
	parser.add_argument('connection.json', nargs=1, help='JSON connection configuration file for data nodes.')
	parser.add_argument('workflow.json', nargs=1, help='JSON workflow configuration for var nodes.')

	parser.add_argument('--loglevel', metavar='LEVEL', choices=['INFO','DEBUG'], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	
	group = parser.add_argument_group('Input data')
	group.add_argument('--input_file', metavar='FILE', default=None, type=argparse.FileType('r'), help="Optional input file. (default: %(default)s")
	group.add_argument('--delim', metavar='CHAR', default='|', help="Input file delimiter. (default: %(default)s")
	group.add_argument('--num_feeders', metavar='NUM', default=1, type=int, help="Number if feeders to run")

	group = parser.add_argument_group('Data nodes')
	group.add_argument('--data_start_port', metavar='PORT', default=9000, type=int, help='Starting HTTP port for data nodes to listen on. (default: %(default)s)')
	group.add_argument('--num_data_nodes', metavar='NUM', default=1, type=int, help='Number of data nodes to run. (default: %(default)s')

	group = parser.add_argument_group('Variable nodes')
	group.add_argument('--var_start_port', metavar='PORT', default=9100, type=int, help='Starting HTTP port for variable nodes to listen on. (default: %(default)s)')
	group.add_argument('--num_var_nodes', metavar='NUM', default=1, type=int, help='Number of variable nodes to run. (default: %(default)s)')
	group.add_argument('--query_parallelism', metavar='PARALLELISM', default=1, type=int, help='Number of queries the variable nodes should run at a time for each request. (default: %(default)s)')
	group.add_argument('--linkages', metavar='LINKAGES', default='test', help='CSV list of linkage queries to run on each variable request. (default: %(default)s)')
	group.add_argument('--skip_linkages', default=False, action='store_true', help='Whether or not to skip linkages. (default: %(default)s')


	group = parser.add_argument_group('HAProxy')
	group.add_argument('--haproxy_stat_port', metavar='PORT', default=1936, type=int, help='HAProxy stat port. (default: %(default)s)')
	group.add_argument('--haproxy_data_port', metavar='PORT', default=8090, type=int, help='HAProxy port for data nodes. (default: %(default)s)')
	group.add_argument('--haproxy_var_port' , metavar='PORT', default=8091, type=int, help='HAProxy port for var nodes. (default: %(default)s)')
	
	args = vars(parser.parse_args())

	logging.basicConfig(level=getattr(logging, args['loglevel']), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

	if args['skip_linkages']:
		args['num_data_nodes'] = 0

	if args['input_file'] != None:
		fname = args['input_file'].name
		args['input_file'].close()
		args['input_file'] = args['input_file'].name

	data_ports = [ args['data_start_port']+i for i in range(args['num_data_nodes']) ]
	var_ports =  [ args['var_start_port']+i  for i in range(args['num_var_nodes'])  ]

	driver = Driver(
		args['queries.json'][0],
		args['connection.json'][0],
		args['workflow.json'][0],
		args['haproxy_stat_port'], 
		args['haproxy_data_port'], 
		data_ports,
		args['haproxy_var_port'], 
		var_ports,
		args['skip_linkages'],
		args['query_parallelism'], 
		args['linkages'],
		args['loglevel'],
		args['input_file'],
		args['delim'],
		args['num_feeders']
	)

	driver.start_all()
	
