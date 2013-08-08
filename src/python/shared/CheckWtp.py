#!/usr/bin/python

import re, os, sys, fileinput, random

if __name__ == "__main__": 

	"""
	Filter out records that do not match the WTP ID
	"""

	wtppatt = r"[0-f]{8}-[0-f]{4}-[0-f]{4}-[0-f]{4}-[0-f]{12}$"
	wtpprog = re.compile(wtppatt)
		
	for oneline in sys.stdin:
		justid = oneline.strip()
		
		if wtpprog.match(justid):
			print justid
	

