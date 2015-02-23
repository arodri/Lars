from datetime import timedelta

class LinkageCrosstab:


	def loadConfigJSON(self,config):
		self.durs = config['durations']
		self.tdDurs = dict([(dur,timedelta(days=dur)) for dur in self.durs])
		self.diffFields = config['diffFields']
		self.setFields = dict([(field,set()) for field in self.diffFields])
		self.linksets= config['linksetsName']
	
	def run(self,record):
		obs = record['obsDate']
		cutOffDates = {}
		for dur in self.durs:
			cutOffDates[dur] = obs-self.tdDurs[dur]
		for (lsName,ls) in record[self.linksets].items():
			durLS = {}
			for dur in self.durs:
				thisSets = dict(self.setFields)
				thisLS = [rec for rec in ls if rec['obsDate']>cutOffDates[dur]]
				for rec in thisLS:
					for field in self.diffFields:
						thisSets[field].add(rec[field])
				for field in self.diffFields:
					record[lsName+"_"+str(dur)+"_diff_"+field] = len(thisSets[field])
				record[lsName+"_"+str(dur)+"_cnt"] = len(thisLS)
		return record
				

				
