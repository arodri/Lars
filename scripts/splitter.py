
import os

size = os.path.getsize('t.psv')
cs = size/10

print size,cs

f = open('t.psv','rb')
c = None
prev = 0
while f.tell() < size and c != "":
	f.seek(prev-1+cs)
	c = f.read(1)
	while c not in ('\n',""):
		c = f.read(1)
	print (prev,min(size,f.tell()-1))
	prev = f.tell()
