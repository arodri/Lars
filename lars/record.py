class record(dict):
	
	def __init__(self, *args, **kw):
		super(record,self).__init__(*args, **kw)
		self.record_id=None

	def set_record_id(self,record_id):
		self.record_id=record_id

	def get_record_id(self):
		return self.record_id
