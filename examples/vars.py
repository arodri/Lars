def run(record):
	for (linkname, ls) in record['linksets'].items():
		record['%s_cnt' % linkname] = len(ls)
	return record

