#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Version of uniq that does not require sorting in advance. 
	Not good with huge inputs, thoug
	"""
	
	myset = set()

	for line in sys.stdin:
		
		x = line.strip()
		
		if len(x) == 0:
			continue
				
		if x in myset:
			continue
			
		print x
		myset.add(x)
