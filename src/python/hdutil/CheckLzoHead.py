#!/usr/bin/python

import re, os, sys, fileinput, random

sys.path.append("/home/burfoot/src/python/shared")

import Util

TMP_OUT_FILE = Util.gimmeTemp()
TMP_ERR_FILE = Util.gimmeTemp()

def checkLzoPath(path2check, N=100):
		
	assert path2check.endswith(".lzo"), "Bad LZO path %s" % (path2check)
	
	hadcall = "hadoop fs -cat %s | lzop -dcqf | head -n %d > %s" % (path2check, N, TMP_OUT_FILE)
	
	#print "Hadcall is %s" % (hadcall)
	
	os.system(hadcall)
	
	lcount = 0
	
	for oneline in open(TMP_OUT_FILE):
		lcount += 1
		
	if lcount < 10:
		print "Read only %d lines from file %s" % (lcount, path2check)
		assert False
		
	os.remove(TMP_OUT_FILE)

if __name__ == "__main__":

	if not len(sys.argv) == 2:
		print "Usage: CheckLzoHead.py <wildcard hdfs path>"
		sys.exit(1)



	hadoopwc = sys.argv[1]
	
	print "wc expression is %s" % (hadoopwc)
	
	hdcall = "hadoop fs -ls %s" % (hadoopwc)
	
	syslist = Util.sysCallResult(hdcall)
	
	if Util.promptOkay("Going to check %d files" % (len(syslist))):
		
		for oneline in syslist:
			toklist = oneline.split(" ")
			
			onepath = toklist[-1]
			
			checkLzoPath(onepath.strip())
			
	print "All %d files checked out okay" % (len(syslist))
