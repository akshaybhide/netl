#!/usr/bin/python

#xxxxxxxxxx

import re, os, sys, fileinput
			
sys.path.append("/local/src/python/util")
sys.path.append("/local/src/python/shared")

import Util
import hadJavaCall

if __name__ == "__main__":

	"""
	Run both the Hadoop mode aggregation AND the KVUploader
	"""
	
	if len(sys.argv) < 2:
		print "HadAggNUpload <daylist.txt>"
		sys.exit(1)
		
	daycodefile = sys.argv[1]	
	
	dayset = set()
	
	for oneline in open(daycodefile):
		dayset.add(oneline.strip())
			
	if not Util.promptOkay("Okay to run for %s?" % (",".join(dayset))):
		print "Aborting"
	
	for oneday in dayset:	
		hadJavaCall.runHadoopCall("HadoopMode", [oneday])
		hadJavaCall.runHadoopCall("KvUploader", [oneday])
