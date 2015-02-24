#!/usr/bin/env python
import signal,sys
from subprocess import Popen
from multiprocessing import Pool
import shlex
import argparse

from jinja2 import Template


class Driver(object):
	def __init__(self, 
		queries_json,
		connection_json,
		workflow_json,
		haproxy_stat_port, 
		haproxy_data_port, data_ports, 
		haproxy_var_port, var_ports,
		query_parallelism, linkages, 
		loglevel):

		self.dataservers = []
		self.haproxies = []
		self.varservers = []

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

	@staticmethod
	def stop_process(process):
		process.terminate()
		process.wait()

	@staticmethod
	def stop_processes(processes):
		if len(processes) > 0:
			#p = Pool(len(processes))
			map(Driver.stop_process, processes)

	def stop_all(self):
		print "Stopping data servers..."
		Driver.stop_processes(self.dataservers)
		print "Done"
		print "Stopping var servers..."
		Driver.stop_processes(self.varservers)
		print "Done"
		print "Stopping haproxy..."
		Driver.stop_processes(self.haproxies)
		print "Done"	

	def signal_handler(self, signal, frame):
		self.stop_all()
		sys.exit(1)
	
	@staticmethod
	def start_dataserver(port, loglevel, queries_config, connection_config):
		with open('logs/data.server.%s.out' % port, 'ab') as out, open('logs/data.server.%s.err' % port, 'ab') as err:
			cmd_str = """python lars/data_node.py
				--http_port=%(port)s
				--log=logs/data.server.%(port)s.log
				--level=%(loglevel)s
				%(connection)s %(queries)s""".replace('\n', ' ').replace('\t', '')
			cmd= shlex.split(cmd_str % {
				'port':port,
				'loglevel':loglevel,
				'connection':connection_config,
				'queries':queries_config
			})
			return Popen(cmd, stdout=out, stderr=err)

	@staticmethod
	def start_haproxy(haproxy_port, data_proxy_port, var_proxy_port, data_ports, var_ports):
		data_servers = zip([ "data%s" % port for port in data_ports ], data_ports)
		var_servers = zip(["var%s" % port for port in var_ports ], var_ports)
		with open('config/haproxy.jinja2.cfg', 'r') as cfg_template:
			with open('logs/haproxy.cfg', 'w') as cfg:
				template = Template(cfg_template.read())
				cfg.write(template.render(stats_port=haproxy_port, data_proxy_port=data_proxy_port, var_proxy_port=var_proxy_port, data_servers=data_servers, var_servers=var_servers))
		cmd = shlex.split("haproxy -f logs/haproxy.cfg")
		with open('logs/haproxy.out','ab') as out, open('logs/haproxy.err', 'ab') as err:
			return Popen(cmd, stdout=out, stderr=err)
	
	@staticmethod
	def start_varserver(port, haproxy_data_port, query_parallelism, linkages, loglevel, workflow_config):
		with open('logs/var.server.%s.out' % port, 'ab') as out, open('logs/var.server.%s.err' % port, 'ab') as err:
			cmd_str = """python lars/var_node.py 
				--http_port=%(port)s 
				--log=logs/var.server.%(port)s.log 
				-o output/var.server.%(port)s.output
				--level=%(loglevel)s
				-p %(query_parallelism)s
				--data_uri=http://127.0.0.1:%(haproxy_data_port)s/api/0.1 
				--linkages=%(linkages)s
				%(workflow)s""".replace('\n', ' ').replace('\t','')
			cmd = shlex.split(cmd_str % {
				'port':port, 
				'haproxy_data_port':haproxy_data_port,
				'linkages':linkages,
				'query_parallelism':query_parallelism,
				'loglevel':loglevel,
				'workflow':workflow_config
				})

			return Popen(cmd, stdout=out, stderr=err)

	def start_all(self):
		signal.signal(signal.SIGINT, self.signal_handler)
		
		try:
			print "Starting dataservers..."
			self.dataservers = [ Driver.start_dataserver(port, self.loglevel, self.queries_json, self.connection_json) for port in self.data_ports ]
			print "Ports: %s" % str(self.data_ports)
			print "Done."
			print "Starting HAProxy..."
			self.haproxies =   [ Driver.start_haproxy(self.haproxy_stat_port, self.haproxy_data_port, self.haproxy_var_port, self.data_ports, self.var_ports) ] 
			print "Ports: %s (stats), %s (var), %s (data)" % (self.haproxy_stat_port, self.haproxy_var_port, self.haproxy_data_port)
			print "Done."
			print "Starting variable servers..."
			self.varservers =  [ Driver.start_varserver(port, self.haproxy_data_port, self.query_parallelism, self.linkages, self.loglevel, self.workflow_json) for port in self.var_ports ]
			print "Ports: %s" % str(self.var_ports)
			print "Done"
		except:
			print ""
			print "OOPS!"
			print ""
			self.stop_all()
			raise

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="Driver program for Lars")

	parser.add_argument('queries.json', nargs=1, help='JSON query configuration file for data nodes.')
	parser.add_argument('connection.json', nargs=1, help='JSON connection configuration file for data nodes.')
	parser.add_argument('workflow.json', nargs=1, help='JSON workflow configuration for var nodes.')

	parser.add_argument('--input_file', metavar='FILE', help='Optional input file to run through.')
	parser.add_argument('--loglevel', metavar='LEVEL', choices=['INFO','DEBUG'], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	
	group = parser.add_argument_group('Data nodes')
	group.add_argument('--data_start_port', metavar='PORT', default=9000, type=int, help='Starting HTTP port for data servers to listen on. (default: %(default)s)')
	group.add_argument('--num_data_nodes', metavar='NUM', default=1, type=int, help='Number of data nodes to run. (default: %(default)s')

	group = parser.add_argument_group('Variable nodes')
	group.add_argument('--var_start_port', metavar='PORT', default=9100, type=int, help='Starting HTTP port for variable servers to listen on. (default: %(default)s)')
	group.add_argument('--num_var_nodes', metavar='NUM', default=1, type=int, help='Number of variable nodes to run. (default: %(default)s)')
	group.add_argument('--query_parallelism', metavar='PARALLELISM', default=1, type=int, help='Number of queries the variable nodes should run at a time for each request. (default: %(default)s)')
	group.add_argument('--linkages', metavar='LINKAGES', default='test', help='CSV list of linkage queries to run on each variable request. (default: %(default)s)')

	group = parser.add_argument_group('HAProxy')
	group.add_argument('--haproxy_stat_port', metavar='PORT', default=1936, type=int, help='HAProxy stat port. (default: %(default)s)')
	group.add_argument('--haproxy_data_port', metavar='PORT', default=8090, type=int, help='HAProxy port for data nodes. (default: %(default)s)')
	group.add_argument('--haproxy_var_port' , metavar='PORT', default=8091, type=int, help='HAProxy port for var nodes. (default: %(default)s)')
	
	args = vars(parser.parse_args())
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
		args['query_parallelism'], 
		args['linkages'],
		args['loglevel']
	)

	driver.start_all()
	signal.pause()
	driver.stop_all()
