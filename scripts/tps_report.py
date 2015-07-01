#!/usr/bin/env python
import time,re,sys,math

#feeder_log = sys.argv[1]
resolution = int(sys.argv[1])
feeder = sys.argv[2].strip().strip('\n')
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

sys.stdout.write('%s\n' % ' '.join([ ' '*(8-len(s))+s for s in ['itr','tps','cnt','total','resp_25', 'rsp_50', 'rsp_75', 'rsp_90', 'resp_95', 'resp_99']]))
sys.stdout.flush()
#with open(feeder_log, 'r') as log:
with sys.stdin as log:
	cnt, i, total = 0, 0, 0
	start, target = None, None

	times = []
	for line in log:
		if logline.match(line):
			(ts, rt) = parse(line)
			cnt += 1
			total += 1
			times.append(rt)
			if start == None:
				start = ts
				target = ts + resolution
			elif target < ts:
				while target < ts:
					times.sort()
					tpss = '%0.2f' % (cnt/float(resolution))
					resp_25 = int(times[min(int(math.floor(len(times)*.25)), len(times)-1)])
					resp_50 = int(times[min(int(math.floor(len(times)*.50)), len(times)-1)])
					resp_75 = int(times[min(int(math.floor(len(times)*.75)), len(times)-1)])
					resp_90 = int(times[min(int(math.floor(len(times)*.90)), len(times)-1)])
					resp_95 = int(times[min(int(math.floor(len(times)*.95)), len(times)-1)])
					resp_99 = int(times[min(int(math.floor(len(times)*.99)), len(times)-1)])
					sys.stdout.write('%s\n' % ' '.join([' '*(8-len(str(v)))+str(v) for v in[ i,tpss,cnt,total,resp_25, resp_50, resp_75, resp_90, resp_95, resp_99]]))
					sys.stdout.flush()
					times = []
					cnt=0
					i+=1
					target += resolution
			#print ts,rt
				


