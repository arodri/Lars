import argparse, os
from lars.feeder import Feeder

parser = argparse.ArgumentParser()
parser.add_argument('input_file',nargs=1,type=argparse.FileType('r'))
parser.add_argument('num_chunks',nargs=1,type=int)
parser.add_argument('-v',default=False, action='store_true', help='Verbose, print every char and index')

args = vars(parser.parse_args())

f = args['input_file'][0]

print Feeder.getOffsets(f,args['num_chunks'][0])

if args['v']:
	size = os.path.getsize(f.name)
	print 'Size: %s' % size
	f.seek(0)
	c = f.read(1)
	while f.tell() <= size and c != "":
		if c == '\n':
			c='\\n'
		print "(%s,%s)" % (f.tell(),c)
		c = f.read(1)

