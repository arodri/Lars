import mapper
import multiprocessing as mt
from multiprocessing import pool
import logging
import dill

# require a different pickler to send the function to a separate process
def run_dill_encoded(what):
	fun, args = dill.loads(what)
	return fun(*args)

def apply_async(pool, fun, args):
	return pool.apply_async(run_dill_encoded, (dill.dumps((fun, args)),))


def mtRun(mtRecord, thisMap):
	return thisMap.runWrapper(mtRecord)

class ParallelMapper(mapper.Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.pool_size = config.get("pool_size", 2)
		self.mapper_configs = config["mappers"]
		self.mapper_defs = []
		self.mapper_provides = set([])

		self.logger = logging.getLogger(self.name)
		self.logger.info("Parsing parallel config")
		for map_config in self.mapper_configs:
			# create the mapper
			thisMapper = mapper.buildFromJSON(map_config)
			thisMapper.verifyParallizable()
			
			# check to see that there are no conflicts in provides
			thisProvides = set(thisMapper.provides)
			intersect = self.mapper_provides.intersection(thisProvides)
			if len(intersect) > 0:
				raise mapper.MapperConfigurationException("%s can not be parallelized in this context, conflict in fields being written to: %s" % (thisMapper.name, ",".join([ str(f) for f in intersect ])))
			self.mapper_provides.update(thisProvides)

			self.logger.info("Built %s" % thisMapper.name)
			self.mapper_defs.append(thisMapper)
		
		self.manager = mt.Manager()
		self.pool = pool.Pool(self.pool_size)

	def run(self,record):
		# need a shared-memory dictionary
		mtRec = self.manager.dict(record)
		
		waits = [ apply_async(self.pool, mtRun, (mtRec, thisMapper)) for thisMapper in self.mapper_defs ]
		for r in waits:
			r.wait()
			r.get()
		
		return dict(mtRec)


