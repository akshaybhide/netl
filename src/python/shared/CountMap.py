#!/usr/bin/python

#xxxxxxxxxx



import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Input is sequence of simple strings (e.g. domains).
	Output is sorted map of counts of those strings
	"""
	
	countmap = {}
	
	for line in sys.stdin:
		
		val = line.strip()
		
		if len(val) == 0:
			continue
		
		countmap.setdefault(val, 0)
		countmap[val] += 1


	countlist = sorted([(countmap[k], k) for k in countmap])
	
	for ctup in countlist:
		print "%s\t%d" % (ctup[1], ctup[0])
