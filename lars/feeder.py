#!/usr/bin/env python
import os

import requests
import logging
import sys
import ujson as json
import argparse
import csv
import time

class Feeder(object):
	def __init__(self, input_file, delim, uri, start_byte=None, end_byte=None, dry_run=False):
		self.logger = logging.getLogger('Feeder')
		self.input_file = input_file
		self.uri = uri
		self.start_byte = start_byte
		self.end_byte = end_byte
		self.dry_run = dry_run

		msg = 'Reading [%s]' % input_file.name
		if start_byte not in [None, 0]:
			msg += ' from byte [%s]' % start_byte
		else:
			msg += ' from start'
		if end_byte == None:
			msg += ' to EOF'
		else:
			msg += ' to byte [%s]' % end_byte
		self.logger.info(msg)

		input_file.seek(0)

		self.logger.debug('Configuring file reader')
		input_file.seek(0)
		field_names = input_file.readline().strip().split(delim)
		#self.reader = csv.DictReader(input_file,delimiter=delim,fieldnames=field_names)
		self.parser = Feeder.makeParser(field_names,delim)
		if start_byte not in [None, 0]:
			self.logger.debug('Seeking to byte %s' % start_byte)
			input_file.seek(start_byte)
		
		self.session = requests.Session()
	@staticmethod
	def makeParser(fieldnames, delim):
		def parser(line):
			return dict(zip(fieldnames,line.strip('\r').strip('\n').split(delim)))
		return parser

	@staticmethod
	def getOffsets(file, num_chunks):
		size = os.path.getsize(file.name)
		chunk_size = size/num_chunks

		chunks = []

		char = None
		prev = 0
		file.seek(0)
		while file.tell() < size and char != "":
			file.seek(prev+chunk_size-1)
			char = file.read(1)
			while char not in ('\n', ''):
				char = file.read(1)
			chunks.append((prev,min(size, file.tell()-1)))
			prev = file.tell()
		file.seek(0)
		return chunks

	def submit(self, record):
		if self.dry_run:
			self.logger.info("%s <- %s" % (self.uri,record))
		else:
			self.logger.debug("%s <- %s" % (self.uri, record))
			start = time.time()
			resp = self.session.post(self.uri, data=json.dumps(record), headers={'Content-Type': 'application/json'})
			end = time.time()
			self.logger.info("%s - [%s] - %s" % (resp.status_code, (end-start)*1000, resp.text))
	
	def run(self):
		if self.end_byte != None:
			self.logger.debug('@%s' % self.input_file.tell())
			while self.input_file.tell() < self.end_byte:
				line = self.input_file.readline()
				self.submit(self.parser(line))
				#self.submit(self.reader.next())
				self.logger.debug('@%s' % self.input_file.tell())
		else:
			#for record in self.reader:
				#self.submit(record)
			for record in map(self.parser, self.input_file):
				self.submit(record)
		sys.exit(0)

if __name__ == '__main__':

	parser = argparse.ArgumentParser(description="A REST-exposed database querying api.")
	
	parser.add_argument('input_file', nargs=1, type=argparse.FileType('r'), help='Delimited input file. REQUIRES HEADER.')
	parser.add_argument('delim', default='|', help='Input file delimiter. (default: %(default)s')

	parser.add_argument('--var_uri', metavar='HOST', default='http://localhost:8091/vars', help='Variable server to submit jobs to.')
	# options
	parser.add_argument('--log', metavar='FILE', default="feeder.log", help='Log file. (default: %(default)s)')
	parser.add_argument('--level', metavar='LEVEL', choices=["INFO","DEBUG"], default='INFO', help='Logging level: %(choices)s (default: %(default)s)')
	

	parser.add_argument('--dry_run', default=False, action='store_true', help="Dry run, don't actually submit the requests, just output them")

	group = parser.add_argument_group('Starting, ending positions', 'USE WITH CARE. If the starting byte is calculated incorrectly, bad things happen.')
	group.add_argument('--start_byte', metavar='INT', default=None, type=int, help='Starting byte. (default: %(default)s)')
	group.add_argument('--end_byte', metavar='INT', default=None, type=int, help='Ending byte. (default: %(default)s')


	args = vars(parser.parse_args())
	
	logging.basicConfig(level=getattr(logging, args['level']), filename=args['log'], filemode='a', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	
	feeder = Feeder(args['input_file'][0], args['delim'], args['var_uri'], args['start_byte'], args['end_byte'], args['dry_run'])

	feeder.run()

