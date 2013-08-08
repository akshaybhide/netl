#!/usr/bin/python

import re, os, sys

if __name__ == "__main__":


	if(len(sys.argv) < 3):
		
		print "Usage ./ThresholdColumnFilter cutoff colid <above|below>"
		exit(1)

	cutoff = float(sys.argv[1])
	colId = int(sys.argv[2])
	above = "above" == sys.argv[3]
	
	for line in sys.stdin:
		
		toks = line.strip().split('\t')
		
		try:
			relval = float(toks[colId])
			
			if above:
				if relval > cutoff:
					print line.strip()
			else:
				if relval < cutoff:
					print line.strip()
	
				
		except Exception:
			pass

