#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Basically transform "____" into tabs
	"""
		
	for line in sys.stdin:
		
		toks = line.strip().split("_____")
		
		print "\t".join(toks);
