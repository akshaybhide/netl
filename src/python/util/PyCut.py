#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Python hack version of cut that rearranges columns
	"""
	
	fieldstr = sys.argv[1]
	cutfields = [int(onef) for onef in fieldstr.split(",")]
	
	#print "Field string is %s" % (fieldstr)
	#print "Cut fields are %s" % (cutfields)
	
	for line in sys.stdin:
		
		toks = line.strip().split("\t")
		
		for onef in cutfields:
			if onef <= len(toks):
				print "%s\t" % toks[onef-1],
				
		print ""

