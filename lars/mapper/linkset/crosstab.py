from datetime import timedelta
from lars.mapper import Mapper
import copy

class DatedCrosstabMapper(Mapper):


	def loadConfigJSON(self,config):
		self.durs = config['durations']
		self.tdDurs = dict([(dur,timedelta(days=dur)) for dur in self.durs])
		self.diffFields = config.get('diffFields',None)
		self.setFields = dict([(field,set()) for field in self.diffFields])
		self.recordLists = config['recordLists']
		self.recordObsDate = config['obsDatetimeField']
		self.recordListObsDate = config['recordListDatetimeField']
		self.missingValue = config.get('missingValue',-1)

		self.provides = []
		for dur in self.durs:
			for rl in self.recordLists:
				for field in self.diffFields:
					self.provides.append("%s_%s_diff_%s" % (rl, dur, field))
					self.provides.append("%s_%s_cnt" % (rl, dur))
	
	def process(self,record):
		obs = record[self.recordObsDate]
		cutOffDates = {}
		for dur in self.durs:
			cutOffDates[dur] = obs-self.tdDurs[dur]
		for (lsName,ls) in [ (rlName, record[rlName]) for rlName in self.recordLists ]:
			durLS = {}
			for dur in self.durs:
				thisSets = dict([(field,set()) for field in self.diffFields])
				if ls != None:
					thisLS = [rec for rec in ls if rec[self.recordListObsDate] > cutOffDates[dur]]
					for rec in thisLS:
						for field in self.diffFields:
							thisSets[field].add(rec[field])
				for field in self.diffFields:
					record[lsName+"_"+str(dur)+"_diff_"+field] = self.missingValue if ls == None else len(thisSets[field])
				record[lsName+"_"+str(dur)+"_cnt"] = self.missingValue if ls == None else len(thisLS)
		return record

class DatedBinaryMapper(Mapper):

	empty = set(["","NULL",None])
	hit = set([1, "1","True","true","t","T",True])
	miss = set([0, "0","False","false","f","F",False])

	def loadConfigJSON(self,config):
		self.durs = config['durations']
		self.tdDurs = dict([(dur,timedelta(days=dur)) for dur in self.durs])
		self.binaryFields = config['binaryFields']
		self.recordLists = config['recordLists']
		self.recordObsDate = config['obsDatetimeField']
		self.recordListObsDate = config['recordListDatetimeField']
		self.fieldCounts = dict([(field,{'1':0,'0':0}) for field in self.binaryFields])
		self.missingValue = config.get('missingValue',-1)

		self.provides = []
		for dur in self.durs:
			for rl in self.recordLists:
				for field in self.binaryFields:
					base = "%s_%s_%s" % (rl, dur, field)
					self.provides.append("%s_rate" % base)
					self.provides.append("%s_cnt" % base)
	
	def process(self,record):
		obs = record[self.recordObsDate]
		cutOffDates = {}
		for dur in self.durs:
			cutOffDates[dur] = obs-self.tdDurs[dur]
		for (lsName,ls) in [ (rlName, record[rlName]) for rlName in self.recordLists ]:
			durLS = {}
			for dur in self.durs:
				binary_fields = dict([(field,{'1':0,'0':0}) for field in self.binaryFields])
				if ls != None:
					thisLS = [rec for rec in ls if rec[self.recordListObsDate]>cutOffDates[dur]]
					for rec in thisLS:
						for field in self.binaryFields:
							v = rec[field]
							if v not in self.empty:
								if v in self.hit:
									binary_fields[field]["1"] += 1
								elif v in self.miss:
									binary_fields[field]["0"] += 1
				for field in self.binaryFields:
					key = "%s_%s_%s" % (lsName, dur, field)

					hits = binary_fields[field]["1"]
					misses = binary_fields[field]["0"]
					rate = -1.0 if hits+misses == 0 else hits/float(hits+misses)
					record["%s_rate" % key] = self.missingValue if ls == None else rate
					record["%s_cnt" % key] = self.missingValue if ls == None else hits
		return record
