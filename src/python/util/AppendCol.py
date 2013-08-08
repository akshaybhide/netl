#!/usr/bin/python

import re, os, sys, fileinput, random

if __name__ == "__main__":

	if not len(sys.argv) == 2:
		print "Usage: AppendCol <col2append>"
		sys.exit(1)
	
	col2append = sys.argv[1]
	
	for line in sys.stdin:
		print "%s\t%s" % (line.strip(), col2append)
