#!/usr/bin/env python
import os

import requests
import logging
import sys
import ujson
import argparse
import csv
import time

import multiprocessing as mt
from http_node import PeriodicTask
from Queue import Empty
class Feeder(mt.Process):
	def __init__(self, q, uri, isDone, id, inputIsJsonStr,):
		super(Feeder, self).__init__()
		self.name = 'Feeder-%s' % id
		self.logger = logging.getLogger(self.name)
		self.q = q
		self.isDone = isDone
		self.isJson = inputIsJsonStr
		self.uri = uri

	def run(self):
		self.logger.info('Started')
		s = requests.Session()
		while not self.isDone.is_set():
			try:
				batch = self.q.get(timeout=1) # wait one second
			except Empty: # empty queue, try again
				continue
			self.logger.debug('Fetched %s records' % len(batch))
			batch = [ (item, 0) for item in batch ] # add info for retries
			for (item,attempts) in batch:
				j = item
				if not self.isJson:
					j = ujson.dumps(item)
				try:
						
					start = time.time()
					resp = s.post(self.uri, data=j, headers={'Content-Type':'application/json'}, timeout=30) # timeout in 30s
					end = time.time()
					self.logger.info('RecordRequest - %s - [%s]' % (resp.status_code, (end-start)*1000))

				except requests.exceptions.Timeout,e:
					logger.warning("RequestTimeout exceded. Retry %s of 3" % attempts)
					attempts += 1
					if attempts < 3:
						batch.append((item,attempts))
						i = attempts/2.0
						time.sleep(i*i)
					else:
						self.logger.exception(e)
				except Exception,e:
					self.logger.exception(e)

def report_qsize(q, logger):
	logger.info('QueueSize=%s'%q.qsize())

def run_feeders(input_file, delim, uri, num_feeders, batch_size, queue_size, input_is_json):
	
	logger = logging.getLogger('FeederControl')
	# create process-safe queue
	q = mt.Queue(queue_size)
	
	# start queue size logger
	p = PeriodicTask(5, report_qsize, q=q, logger=logger)
	p.run()

	# start up feeders
	logger.info('Starting feeders')
	feeders = []
	for i in range(num_feeders):
		isDone = mt.Event()
		
		f = Feeder(q, uri, isDone, i, input_is_json)
		f.daemon = True
		f.start()
		feeders.append((f, isDone ))

	# start reading
	logger.info('Reading input')
	reader = input_file
	if not input_is_json:
		reader = csv.DictReader(input_file, delimiter=delim)
	
	batch = []
	for record in reader:
		batch.append(record)
		if len(batch) >= batch_size:
			q.put(batch)
			batch = []
	
	# wait for the queue to be empty
	logger.info('All input queued, waiting for it to be drained')
	while not q.empty():
		time.sleep(.25)

	logger.info('Stopping feeders')
	# wait for the feeders to be done
	for (feeder, isDone) in feeders:
		isDone.set()
	logger.info('Feeders told to stop')
	logger.info('Joining feeder threads')
	for (feeder, isDone) in feeders:
		feeder.join()
	logger.info('All done!')

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="A REST-exposed database querying api.")
	
	parser.add_argument('input_file', nargs=1, type=argparse.FileType('r'), help='Delimited input file. REQUIRES HEADER.')
	parser.add_argument('delim', default='|', help='Input file delimiter. If the file is already in JSON format (one line per record) specify "JSON". (default: %(default)s')
	parser.add_argument('--var_uri', metavar='HOST', default='http://localhost:8091/vars', help='Variable server to submit jobs to.')
	# options
	group = parser.add_argument_group('Logging')
	parser.add_argument('--log', metavar='FILE', default=None, help='Log file. (default: %(default)s)')
	parser.add_argument('--loglevel', metavar='LEVEL', choices=["INFO","DEBUG","WARNING","ERROR"], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	

	group = parser.add_argument_group('Performance tuning.')
	group.add_argument('--num_feeders', metavar='INT', default=1, type=int, help='Number of processes to spin up. (default: %(default)s')
	group.add_argument('--batch_size', metavar='INT', default=1, type=int, help='Batch size that each feeder will process. (default: %(default)s')
	group.add_argument('--queue_size',metavar='INT',default=5000, type=int, help='Maximum length of queue, measured in number of batches. (default: %s(default)s')


	args = parser.parse_args()
	logging_config = dict(level=getattr(logging, args.loglevel), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	if args.log != None:
		logging_config['filename'] = args.log
		logging_config['filemode'] = 'a'
	
	logging.basicConfig(**logging_config)
	
	input_is_json = args.delim in ('JSON','json','Json')
	run_feeders(args.input_file[0], args.delim, args.var_uri, args.num_feeders, args.batch_size, args.queue_size, input_is_json)


