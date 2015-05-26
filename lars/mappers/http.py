import requests
import functools
import json

from mapper import Mapper


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

		self._http_partial = functools.partial(self.http_func, self.base_url)
		
		# configure the base http_request function to just apply the action to the URL
		#  no params, no data
		self.http_request = lambda data: self._http_partial()
		self.is_json = False

		self.provides = [self.output_key, self.response_code_key]

	def setRequestToJSON(self):
		self.is_json = True
		self.session.headers.update({'Content-Type':'application/json'})
		self.http_request = lambda data: self._http_partial(data=data)

	def setRequestToParams(self):
		self.is_json = False
		self.http_request = lambda data: self._htt_partial(params=data)

	def getResponse(self, data_or_params=None):
		return self.http_request(data_or_params)

	def makeRequestData(self,record):
		raise NotImplementedError, 'makeRequest method on HTTPRequest has not been implemented'

	def process(self, record):
		raise NotImplementedError, 'process method on HTTPRequest object has not been implemented'



class JSONRequest(HTTPRequest):
	

	def loadConfigJSON(self,config):
		#super(JSONRequest, self).loadConfigJSON(config)
		HTTPRequest.loadConfigJSON(self, config)
		self.setRequestToJSON()

	def process(self,record):
		j = self.makeRequestData(record)
		resp = self.getResponse(json.dumps(j))
		record[self.response_code_key] = resp.status_code
		record[self.output_key] = resp.json()
		return record

	def makeRequestData(record):
		return record


class MelissaRequest(JSONRequest):

	def loadConfigJSON(self,config):
#		super(MelissaRequest, self).loadConfigJSON(config)
		JSONRequest.loadConfigJSON(self, config)

		fields = config['fields']
		self.address1 = fields['address1']
		self.address2 = fields['address2']
		self.city = fields['city']
		self.state = fields['state']
		self.zip = fields['zip']

	def makeRequestData(self, record):
		a = record['raw_address'][0]
		return {
			'Address':a.get(self.address1, ""),
			'Address2':a.get(self.address2, ""),
			'City':a.get(self.city, ""),
			'State':a.get(self.state, ""),
			'Zip':a.get(self.zip)
		}

