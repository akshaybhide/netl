#!/usr/bin/python

import re, os, sys, fileinput

localFilePathGz = "localBidLog.gz"
	
def getFileFromS3(fullBucketPath):
	"""
	Copies files to local
	"""

    	get_cmd = "hadoop fs -get s3n://adnetik-uservervillage/%s %s" % (fullBucketPath, localFilePathGz)
    	os.system(get_cmd)	

def deleteLocalFile():
	
	delCall = "rm -f %s" % (localFilePathGz)
	
	os.system(delCall)

def downloadProcess(callbackFunc):

	runCount = 0

	for line in sys.stdin:
		val = line.strip()
		#print "val is %s" % (val)
		toks = val.split(".")
		
		if(toks.pop() != 'gz'):
			continue
		
		deleteLocalFile()		
		getFileFromS3(val)
		lineLen = callbackFunc(val)
		
		if(runCount > 5):
			break
			
		runCount += 1
	
