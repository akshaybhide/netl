#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	toput = sys.argv[1]

	cwd = os.getcwd()
	hadcall = "hadoop fs -put %s %s/%s" % (toput, cwd, toput)
	
	print hadcall
	os.system(hadcall)
