

#everything currently done by convention
LINKSETS="linksets"

##########
#takes in a dictionary, spits out a dictionary. 
#Input dictionary represents a single request which already has all the field required
#in this particular circumstance has linksets in it
#output dictionary corresponds to adding whatever fields needed adding 
########
def run(result):
	for (lsName,ls) in result[LINKSETS].items():
		result[lsName+"_cnt"] = len(ls)
	return result
	
