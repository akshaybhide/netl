#!/usr/bin/python

import re, os, sys, fileinput, random

if __name__ == "__main__":

	if(len(sys.argv) < 3):
		
		print "Usage ./SetFilterColumn searchFile colId"
		exit(1)

	inFile = sys.argv[1]
	colId = int(sys.argv[2])

	# set of ids to match against
	idset = set()

	for line in open(sys.argv[1]):
		idset.add(line.strip())
		
	
	for line in sys.stdin:
		
		toks = line.strip().split('\t')
		
		try:
			reltok = toks[colId]
			
			if reltok in idset:
				print line.strip()		
				
		except Exception:
			pass

			