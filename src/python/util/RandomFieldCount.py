#!/usr/bin/python

import re, os, sys, fileinput, random
			
if __name__ == "__main__":

	"""
	Same as FieldCount, except only print out a random subset of all 
	the input data.
	"""

	field_id = int(sys.argv[1])
	cutoff = int(sys.argv[2])
	
	for line in sys.stdin:
		
		elems = line.strip().split('\t')
		
		if len(elems) <= field_id:
			continue
		
		if len(elems[field_id]) == 0:
			continue
			
		if random.randint(0, 1000) < cutoff:
			print "%s\t1" % (elems[field_id])
