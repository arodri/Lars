from mapper import Mapper

class Populated(Mapper):
	_DEFAULT_MISSING_VALUES = ['', 'NULL', '\N']

	def loadConfigJSON(self,config):
		self.check_fields = config['check_fields']
		self.missing_values = set(config.get('missing_values', self._DEFAULT_MISSING_VALUES))

	def process(self,record):
		for field in self.check_fields:
			key = "%s_isPopulated" % field
			if field not in record:
				record[key] = -1
			elif record[field] in self.missing_values:
				record[key] = 0
			else:
				record[key] = 1
		return record
