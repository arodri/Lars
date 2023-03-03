from lars.workflow import Workflow
import json
import sys

def getProvides(config):
	wf = Workflow()
	wf.buildJSON(config)
	res = {}
	for mapper,_ in wf.mappers:
		print(mapper)
		res[mapper.name] = mapper.provides
	return res
	


res =  getProvides(json.load(sys.stdin))
print(json.dumps(res,indent=4, separators=(",", ":")))



