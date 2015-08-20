import logging
import socket

JSON_FORMAT="""{
	"time":"%(asctime)s",
	"host":"%(hostname)s",
	"applicationname":"%(applicationname)s",
	"workflow":"%(workflow)s",
	"name":"%(name)s",
	"level":"%(levelname)s",
	"requestid":"%(record_id)s",
	"message":"%(message)s"
}""".replace('\n','').replace('\t',' ')

BASIC_FORMAT='%(asctime)s - %(name)s - %(record_id)s - %(levelname)s - %(message)s'

class LarsFormatter(logging.Formatter):
	
	def format(self, record):
		#return logging.Formatter.format(self, record)
		return logging.Formatter.format(self, LarsFormatter.with_context(record))

	def formatTime(self, record, datefmt=None):
		#return logging.Formatter.formatTime(self, record, datefmt)
		return logging.Formatter.formatTime(self, LarsFormatter.with_context(record), datefmt)

	@staticmethod
	def with_context(record):
		rd = record.__dict__
		if 'record_id' not in rd:
			rd['record_id'] = 'UNKNOWN'
		if 'hostname' not in rd:
			rd['hostname'] = socket.gethostname()
		if 'applicationname' not in rd:
			rd['applicationname'] = 'lars'
		if 'workflow' not in rd:
			rd['workflow'] = 'UNKNOWN'
		return record

class LarsLoggerAdapter(logging.LoggerAdapter):

	def process(self,msg,kwargs):
		kwargs['extra'] = self.extra
		return msg,kwargs

	def setContext(self,context):
		self.extra = context

def configure_file(filename, filemode='w', level='INFO', fmt=BASIC_FORMAT):
	f = LarsFormatter(fmt)

	hdlr = logging.StreamHandler()
	hdlr.setFormatter(f)

	rl = logging.getLogger()
	rl.setLevel(level)
	rl.addHandler(hdlr)

def configure_stderr(level='INFO', fmt=BASIC_FORMAT):
	f = LarsFormatter(fmt)

	hdlr = logging.StreamHandler()
	hdlr.setFormatter(f)

	rl = logging.getLogger()
	rl.setLevel(level)
	rl.addHandler(hdlr)

def configure_basic_stderr(level='INFO'):
	configure_stderr(level=level, fmt=BASIC_FORMAT)

def configure_json_stderr(level='INFO'):
	configure_stderr(level=level, fmt=JSON_FORMAT)

def configure_basic_file(level='INFO'):
	configure_file(level=level, fmt=BASIC_FORMAT)

def configure_json_file(level='INFO'):
	configure_file(level=level, fmt=JSON_FORMAT)
