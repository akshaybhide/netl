#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Split a file by tab, pick out a particular field as a key, 
	then print <key, 1>. 
	This can be sent to pyreducecount to count the number of appearances
	of that field.
	Does not print data for a line if there is not enough tokens in the line,
	or if the field length is zero.
	"""

	field_id = int(sys.argv[1])
	
	for line in sys.stdin:
		
		elems = line.strip().split('\t')
		
		if len(elems) <= field_id:
			continue
		
		if len(elems[field_id]) == 0:
			continue
		
		print "%s\t1" % (elems[field_id])
