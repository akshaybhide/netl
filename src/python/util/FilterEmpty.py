#!/usr/bin/python

#xxxxxxxxxx



import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Filters out rows that have an empty field 
	With no arguments, filters out empty rows.
	With one argument, looks up given field, and filters if that field is empty
	"""
	
	colid = None
	
	if len(sys.argv) > 1:
		colid = int(sys.argv[1])
		
			
	for line in sys.stdin:
		
		relstr = line.strip()
		
		if colid != None:
			toks = line.split("\t")
			relstr = toks[colid]
			
		# Print whole line if relstr is non-empty
		if len(relstr.strip()) > 0:
			print line,

