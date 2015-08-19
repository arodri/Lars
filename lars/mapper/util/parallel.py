from lars import mapper
import logging
import concurrent.futures

class FuturesMapper(mapper.Mapper):

	def loadConfigJSON(self,config):
		self.pool_size = config.get("pool_size", 20)
		self.mapper_configs = config["mappers"]
		self.mapper_defs = []
		self.mapper_provides = set([])

		self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.pool_size)

		self.logger.info("Parsing parallel config")

		for map_config in self.mapper_configs:
			# create the mapper
			builder = mapper.JSONMapperBuilder(map_config)
			(thisMapper,skip) = builder.getMapper(self.logger.extra)
			if thisMapper.skip:
				self.logger.warning("Skipping %s" % thisMapper.name)
			else:
			
				# check to see that there are no conflicts in provides
				thisMapper.verifyParallizable()

				thisProvides = set(thisMapper.provides)
				intersect = self.mapper_provides.intersection(thisProvides)
				if len(intersect) > 0:
					raise mapper.MapperConfigurationException("%s can not be parallelized in this context, conflict in fields being written to: %s" % (thisMapper.name, ",".join([ str(f) for f in intersect ])))
				self.mapper_provides.update(thisProvides)
	
				self.logger.info("Built %s" % thisMapper.name)
				self.mapper_defs.append(thisMapper)

	def stop(self):
		self.executor.shutdown()

	def process(self, record):
		results = []
		for tMapper in self.mapper_defs:
			self.logger.debug("Submitting %s" % tMapper.name)
			f = self.executor.submit(tMapper.processWrapper, record, True, True)
			results.append((tMapper, f))

		for (tMapper, f) in results:
			self.logger.debug("Waiting for %s" % tMapper.name)
			f.result()
		return record

