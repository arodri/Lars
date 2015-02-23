import multiprocessing
import sys

def cut(r):
	return r.strip().split('|')[10]

#p = multiprocessing.Pool(3)
#for line in p.imap(cut, sys.stdin, 100000):
#	print line
for line in sys.stdin:
	print cut(line)
