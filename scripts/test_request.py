import requests
import uuid

from datetime import datetime
from tornado.httpclient import AsyncHTTPClient
from tornado import ioloop

timings = []
cnt = 200

full_start=datetime.now()
for i in range(cnt):
	start = datetime.now()
	requests.get('http://localhost:9113/api/0.1/test?primary_id=%s&uuid=%s' % (i, uuid.uuid4().int))
	end = datetime.now()
	timings.append((end-start).seconds*1000 + (end-start).microseconds/1000.0)
full_end=datetime.now()
print "%s in %s seconds" % (cnt, (full_end-full_start).seconds)
print "tps: %s" % str(cnt/float((full_end-full_start).seconds))
""

timings.sort()
timings.reverse()
indexes = range(0,cnt,int(cnt/10)) + range(int(cnt*.9), cnt, (cnt-int(cnt*.9))/10)[1:] + [cnt-1]
percentiles = range(0,100,10) + range(90,100)[1:] + [100]

for (idx,perc) in zip(indexes,percentiles):
	print "%sth - %s" % (perc, timings[idx])

