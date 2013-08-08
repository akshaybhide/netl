#!/usr/bin/python

import re, os, sys, fileinput, random

if __name__ == "__main__":

	if not len(sys.argv) == 3:
		print "Usage: ApplyPrefSuf <pref> <suf>"
		sys.exit(1)
	

	pref = sys.argv[1]
	suff = sys.argv[2]
	
	for line in sys.stdin:
		
		print "%s%s%s" % (pref, line.strip(), suff)
