import json,sys
for line in sys.stdin:
	j = json.loads(line)
	k = list(j.keys())
	k.sort()
	print('|'.join(k))
