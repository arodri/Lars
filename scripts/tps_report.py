import time,re,sys

#feeder_log = sys.argv[1]
resolution = int(sys.argv[2])
feeder = sys.argv[3].strip().strip('\n')
if feeder=='all':
	logline = re.compile('^.* - Feeder-.+ - RecordRequest - 200 - \[.+\]$')
else:
	logline = re.compile('^.* - Feeder-'+feeder+' - INFO - RecordRequest - 200 - \[.+\]$')
	

start = None
cnt = 0
i = 0
rsp = 0
def parse(line):
	parsed = line.strip('\r').strip('\n').split(' - ')
	ts = time.mktime(time.strptime(parsed[0], '%Y-%m-%d %H:%M:%S,%f'))
	response_time = float(parsed[5].strip('[').strip(']'))
	return (ts,response_time)

sys.stdout.write('%s\n' % ' '.join(['itr','tps','cnt','total','avg_rsp']))
sys.stdout.flush()
#with open(feeder_log, 'r') as log:
with sys.stdin as log:
	cnt, i, total = 0, 0, 0
	start, target = None, None

	for line in log:
		if logline.match(line):
			(ts, rt) = parse(line)
			cnt += 1
			total += 1
			rsp += rt
			if start == None:
				start = ts
				target = ts + resolution
			elif target < ts:
				while target < ts:
					sys.stdout.write('%s\n' % ' '.join([str(v) for v in[ i,cnt/float(resolution),cnt,total,rsp/float(cnt)]]))
					sys.stdout.flush()
					cnt = 0
					rsp = 0
					i += 1
					target += resolution
			#print ts,rt
				


