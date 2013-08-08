#!/usr/bin/python

import sys
			
if __name__ == "__main__":

	"""
	Set some fields to an empty string
	"""
	
	
	scrubcols = sys.argv[1].split(',')

	for line in sys.stdin:
		
		elems = line.split('\t')
		
		for sc in scrubcols:
			elems[int(sc)] = ""
		
		print "\t".join(elems),
