#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Count the number of columns in each line of a file
	"""
	cset = set()
	
	for line in sys.stdin:
		
		elems = line.strip().split('\t')
		
		ccount = len(elems)
		
		if not ccount in cset:
			print ccount
			cset.add(ccount)
			
		
