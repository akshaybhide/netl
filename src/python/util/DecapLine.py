#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Just cut off the first element of a line
	"""

	for line in sys.stdin:
		
		elems = line.strip().split('\t')
		elems = elems[1:]
		print "\t".join(elems)
