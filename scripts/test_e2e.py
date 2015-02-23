import requests
import uuid

from requests import Session

from datetime import datetime
from tornado.httpclient import HTTPClient
from tornado import ioloop
from multiprocessing import Pool
import random
import json

timings = []
cnt = 200

#http_client=HTTPClient()
full_start=datetime.now()
def handle_response(response):
	response.body
	ioloop.IOLoop.instance().stop()

s = Session()

def submit_request(i):
	global timings
	start = datetime.now()
	r = random.expovariate(8*3)
	#r=1
	#s.get('http://localhost:8080/api/0.1/slowtest?primary_id=%s&uuid=%s&sleep_time=%s' % (i, uuid.uuid4().int, r))
	
	r = json.dumps({'primary_id':i, 'sleep_time': r, 'uuid': uuid.uuid4().int, 'passthrough':'gerg'})
	result = s.post('http://localhost:8081/vars', data=r, headers={'Content-type': 'application/json'})
	print result.status_code
	#ioloop.IOLoop.instance().start()
	end = datetime.now()
	return ((end-start).seconds*1000 + (end-start).microseconds/1000.0)
poolsize=5
p = Pool(poolsize)

# parallel
timings = p.map(submit_request, range(cnt))

# sequential
#timings = map(submit_request, range(cnt))

# bucket batched queries
#ids = range(0,cnt)
#timings = [ sum(p.map(submit_request, group)) for group in [ ids[i:i+poolsize] for i in xrange(0, cnt, poolsize) ]]
#cnt = len(timings)


s.close()

full_end=datetime.now()
print "%s in %s seconds" % (cnt, (full_end-full_start).seconds)
print "tps: %s" % str(cnt/float((full_end-full_start).seconds))

print len(timings)

timings.sort()
indexes = range(0,cnt,int(cnt/10)) + range(int(cnt*.9), cnt, (cnt-int(cnt*.9))/10)[1:] + [cnt-1]
percentiles = range(0,100,10) + range(90,100)[1:] + [100]

for (idx,perc) in zip(indexes,percentiles):
	print "%sth - %s" % (perc, timings[idx])



