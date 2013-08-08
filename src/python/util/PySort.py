#!/usr/bin/python

import sys

if __name__ == "__main__":

	if len(sys.argv) < 1:
		print "Usage: PySort.py colid [numeric]" 
		sys.exit(1)
		
	
	numeric = ("".join(sys.argv).find("numeric") > -1)
	
	colid = int(sys.argv[1])	
	tuplist = []
	
	for line in sys.stdin:
		toks = line.split('\t')
		tup = (int(toks[colid]) if numeric else toks[colid], line)
		tuplist.append(tup)
		
	for tup in sorted(tuplist):
		print tup[1],
