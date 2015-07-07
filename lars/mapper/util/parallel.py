import mapper
import multiprocessing as mt
from multiprocessing.pool import ThreadPool
import logging
import dill
import threading

# require a different pickler to send the function to a separate process
def run_dill_encoded(what):
	fun, args = dill.loads(what)
	return fun(*args)

def apply_async(pool, fun, args):
	return pool.apply_async(run_dill_encoded, (dill.dumps((fun, args)),))


def mtRun(mtRecord, thisMap):
	return thisMap.processWrapper(mtRecord)


class ParallelProcessMapper(mapper.Mapper):

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
			thisMapper,skip = mapper.JSONMapperBuilder.buildFromJSON(map_config)
			if skip:
				self.logger.warning("Skipped mapper: %s" % thisMapper.name)
			else:
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
		self.pool = ThreadPool(self.pool_size)

	def process(self,record):
		# need a shared-memory dictionary
		mtRec = self.manager.dict(record)
		
		waits = [ apply_async(self.pool, mtprocess, (mtRec, thisMapper)) for thisMapper in self.mapper_defs ]
		for r in waits:
			r.wait()
			r.get()
		
		return dict(mtRec)

class ThreadSharedRecord:
	def __init__(self):
		self.record = {}

class ThreadedMapper(threading.Thread):
	def __init__(self, semaphore, recordReady, shared_record, mapperBuilder):
		self.semaphore = semaphore
		self.recordReady = recordReady
		self.isDone = threading.Event()
		self.shared_record = shared_record

		self._stop = threading.Event()

		(self.mapper, self.skip) = mapperBuilder.getMapper()
		self.logger = logging.getLogger(self.mapper.name)

		super(ThreadedMapper, self).__init__(name=self.mapper.name)
		self.daemon = True

	def getProvides(self):
		self.mapper.verifyParallizable()
		return self.mapper.provides

	def stop(self):
		self._stop.set()

	def stopped(self):
		return self._stop.isSet()

	def run(self):
		self.logger.debug("Waiting for record - first time")
		self.recordReady.acquire()
		self.recordReady.wait() # automatically releases
		while not self.stopped():
			self.logger.debug("Woo, record is ready!")
			# make sure only X number of threads are running at a time
			self.logger.debug("Waiting for semaphore")
			self.semaphore.acquire()
			self.logger.debug("I get to run!")
			
			# do the work
			self.mapper.processWrapper(self.shared_record.record, True, False)
			
			# let another mapper run
			self.logger.debug("Done! releasing semaphore")
			self.semaphore.release()

			# tell the parent that this thread is all done
            # but first get the recordReady lock to avoid missing a new record
			self.logger.debug("Acquiring recordReady lock")
			self.recordReady.acquire()

			self.logger.debug("Notifying parent I'm done.")
			self.isDone.set()
			self.logger.debug("Parent notified.")
            
			# wait until the shared_record is ready
			self.logger.debug("Waiting for record")
			self.recordReady.wait()


class ParallelThreadMapper(mapper.Mapper):

	def loadConfigJSON(self,config):
		self.name = config["name"]
		self.pool_size = config.get("pool_size", 2)
		self.mapper_configs = config["mappers"]
		self.mapper_defs = []
		self.mapper_provides = set([])

		self.logger = logging.getLogger(self.name)
		self.logger.info("Parsing parallel config")

		self.shared_record = ThreadSharedRecord()
		thread_pool = threading.BoundedSemaphore(self.pool_size)
		self.recordReady = threading.Condition()
		for map_config in self.mapper_configs:
			# create the mapper
			builder = mapper.JSONMapperBuilder(map_config)
			thisMapper = ThreadedMapper(thread_pool, self.recordReady, self.shared_record, builder)
			if thisMapper.skip:
				self.logger.warning("Skipping %s" % thisMapper.name)
			else:
			
				# check to see that there are no conflicts in provides
				thisProvides = set(thisMapper.getProvides())
				intersect = self.mapper_provides.intersection(thisProvides)
				if len(intersect) > 0:
					raise mapper.MapperConfigurationException("%s can not be parallelized in this context, conflict in fields being written to: %s" % (thisMapper.name, ",".join([ str(f) for f in intersect ])))
				self.mapper_provides.update(thisProvides)
	
				self.logger.info("Built %s" % thisMapper.name)
				self.mapper_defs.append((thisMapper,thisMapper.isDone))
		for (tMapper,isDone) in self.mapper_defs:
			tMapper.start()

	def stop(self):
		for tMapper,isDone in self.mapper_defs:
			tMapper.stop()

	def process(self,record):
		# need a shared-memory dictionary
		self.shared_record.record = record

		# tell all the mappers that the record is ready
		self.logger.debug("Notifying threads the record is ready")
		self.recordReady.acquire()
		self.recordReady.notifyAll()
		self.recordReady.release()
		self.logger.debug("Threads notified")

		# wait for the threads to be done
		self.logger.debug("Waiting for threads to finish")
		for (tMapper,isDone) in self.mapper_defs:
			self.logger.debug("Waiting for %s" % tMapper.name)
			isDone.wait()
			isDone.clear()
			self.logger.debug("%s is done" % tMapper.name)
		self.logger.debug("Threads finished")

		return self.shared_record.record

