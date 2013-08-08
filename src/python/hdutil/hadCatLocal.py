#!/usr/bin/python

import re, os, sys, fileinput
			
def getPartList(fname, cwd):
	
	resfile = "hadoop_%s.txt.tmp" % (fname)
	cwd = os.getcwd()
	hadcall = "hadoop fs -ls %s/%s > %s" % (cwd, fname, resfile)	
	os.system(hadcall)
	
	hlines = [line for line in open(resfile) if line.find("part-000") > -1]	
	os.remove(resfile)
	return [line.strip().split(" ")[-1] for line in hlines]	
	
if __name__ == "__main__":

	if len(sys.argv) < 2:
		print "Usage hadCatLocal.py <local_dir>"
		sys.exit(1)

	fname = sys.argv[1]
	cwd = os.getcwd()
	
	for pfile in getPartList(fname, cwd):
		hadcall = "hadoop fs -cat %s" % (pfile)
		#print hadcall
		os.system(hadcall)

