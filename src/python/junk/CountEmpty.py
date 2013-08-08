#!/usr/bin/python

#xxxxxxxxxx



import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Count Number of empty fields
	"""
	
	ecount = 0
	tcount = 0
		
	for line in sys.stdin:
		
		tcount += 1
		
		if len(line.strip()) == 0:
			ecount += 1
			
	print "Found  %d empty records out of %d total" % (ecount, tcount)
