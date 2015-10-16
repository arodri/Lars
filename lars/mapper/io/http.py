import requests
import functools
import json
from lars.mapper import Mapper
from lars.util import PeriodicTask

class HTTPRequest(Mapper):
	

	valid_actions = ['GET','POST']

	def loadConfigJSON(self,config):
		self.base_url = config['base_url']
		self.action = config['action']
		self.output_key = config['output_key']
		self.response_code_key = config['response_code_key']

		if self.action not in self.valid_actions:
			raise AttributeError, 'Invalid action: %s' % self.action

		self.session = requests.Session()
		
		self.http_func = None
		if self.action == 'GET':
			self.http_func = self.session.get
		if self.action == 'POST':
			self.http_func = self.session.post
                ping = config.get("ping",30)
                if ping > 0:
                    t = PeriodicTask(ping, self.arod_request,jitter=.25)
                    t.run()
		self.provides = [self.output_key, self.response_code_key]

        def arod_request(self):
            self.session.options(self.base_url)
            self.logger.info("connection heartbeat for %s" % self.base_url)
            

	def setContentToJSON(self):
		self.session.headers.update({'Content-Type':'application/json'})
	
	def getResponse(self, data=None, params=None):
		return self.http_func(self.base_url, data=data, params=params)

	def makeRequestData(self,record):
		raise NotImplementedError, 'makeRequest method on HTTPRequest has not been implemented'

	def process(self, record):
		raise NotImplementedError, 'process method on HTTPRequest object has not been implemented'

class JSONRequest(HTTPRequest):
	

	def loadConfigJSON(self,config):
		#super(JSONRequest, self).loadConfigJSON(config)
		HTTPRequest.loadConfigJSON(self, config)
		self.setContentToJSON()

	def process(self,record):
		j = self.makeRequestData(record)
		resp = self.getResponse(data=json.dumps(j))
		#resp_code = resp.code
		resp_code = resp.status_code
		if resp_code != 200:
			self.logger.error("http_code: %s" % resp_code)
			self.logger.error(resp.text)
			#self.logger.error(resp.body)
			if resp.status_code >= 400:
				raise requests.exceptions.HTTPError("HTTP_CODE: %s; %s" % (resp_code, resp.text))
				#raise requests.exceptions.HTTPError("HTTP_CODE: %s; %s" % (resp_code, resp.body))
		record[self.response_code_key] = resp_code
		record[self.output_key] = resp.json()
		#record[self.output_key] = json.loads(resp.body)
		return record

	def makeRequestData(record):
		return record

