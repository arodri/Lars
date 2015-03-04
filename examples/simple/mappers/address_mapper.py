import logging
from mapper import Mapper

class AddressKeyMapper(Mapper):
	badAddKey = "-1"
	

	def loadConfigJSON(self,config):
		#takes a list of fields will concatenate all to make street address
		self.streetAddressFields = config['addressFields']
		self.zipField = config['zip']
		self.addKeyField = config['addressKey']
		self.logger = logging.getLogger("address_key_mapper")


	def run(self,record):
		thisAdd = ""
		addKey = None
		for field in self.streetAddressFields:
			thisAdd += record[field]
		parseAdd = thisAdd.upper().split(" ")
		try:
			#check to see if the first part is a number
			int(parseAdd[0])
		except Exception:
			addKey = AddressKeyMapper.badAddKey
			self.logger.info("first part of address not have a number")
		if len(parseAdd)==1:
			addKey = AddressKeyMapper.badAddKey
			self.logger.info("address does not have any spaces")
		if len(record[self.zipField]) <5:
			addKey = AddressKeyMapper.badAddKey
			self.logger.info("zip code is not at least five digits")
		#actually construct addressKey
		if addKey != AddressKeyMapper.badAddKey:
			addKey = parseAdd[0]+parseAdd[1][:2]+record[self.zipField][:5]
		record[self.addKeyField] = addKey
		return record
			
			
