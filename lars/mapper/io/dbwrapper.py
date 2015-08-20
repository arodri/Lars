import importlib

class DBWrapper():
	def __init__(self, config):
		self.engine_url = config['engine_url']
		self.query_pool_size = config.get('query_pool_size',2)
		self.query_logging = config.get('query_logging', False)
	
	def execute(self, sql, params={}):
		raise NotImplementedError

def get_wrapper(config):
	wrapper_class = config["db_class"]

	parsed_class_path = wrapper_class.split(".")
	module_name,class_name = ('.'.join(parsed_class_path[:-1]), parsed_class_path[-1])
	thisMod = importlib.import_module(module_name)
	thisClass = getattr(thisMod, class_name)
	thisWrapper = thisClass(config)

	return thisWrapper

