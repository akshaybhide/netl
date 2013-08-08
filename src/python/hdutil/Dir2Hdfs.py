#!/usr/bin/python

import re, os, sys, fileinput, random

if __name__ == "__main__":

	if not len(sys.argv) == 2:
		print "Usage: Dir2Hdfs.py <hdfsdir>"
		sys.exit(1)

	#doIt(os.getcwd(), "/userindex/testmani")

	# This is kind of the hacky way to do things
	sys.path.append("/local/src/cronjobs/")
	import SynchUtil

	hdfsdir = sys.argv[1]	
	
	ftypemap = {}
	
	for onefile in os.listdir("."):
				
		ftoks = onefile.split(".")
		
		if not len(ftoks) == 2:
			continue
			
		basename = ftoks[0]
		ftype = ftoks[1]

		ftypemap.setdefault(ftype, 0)
		ftypemap[ftype] += 1
	
	
	for ftype in ftypemap:
		print "Found %d files of type %s" % (ftypemap[ftype], ftype)

	if not SynchUtil.promptOkay("Going to upload files to %s" % (hdfsdir)):	
		sys.exit(1)
		
	for onefile in os.listdir("."):
		
		upcall = "hadoop fs -put %s %s" % (onefile, hdfsdir)
		print "Upload call is %s" % (upcall)
		os.system(upcall)
