from lars.mapper import Mapper

class DedupeLS(Mapper):

	def loadConfigJSON(self,config):
		self.ls_names = config["linkset_names"]
		self.dedupe_fields = config["dedupe_fields"]
		self.deduped_ls = config.get("deduped_linksets",self.ls_names)
		assert len(self.ls_names) == len(self.deduped_ls)

	def process(self,record):
		for i, linkset in enumerate(self.ls_names):
			records_seen = []
			deduped_ls = []
			if record[linkset] is not None:
				for link in record[linkset]:
					link_dedupe_fields = []
					for field in self.dedupe_fields:
						link_dedupe_fields.append(link[field])
					if link_dedupe_fields not in records_seen:
						records_seen.append(link_dedupe_fields)
						deduped_ls.append(link)
					else:
						pass
						#do nothing because this is a dupe
			else:
				deduped_ls = record[linkset]
			record[self.deduped_ls[i]] = deduped_ls
		return record
