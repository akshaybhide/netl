#!/usr/bin/python

#xxxxxxxxxx

import re, os, sys, fileinput
			
sys.path.append("/local/src/python/util")
import hadJavaCall

if __name__ == "__main__":

	"""
	Runs the HadoopMode backfill for a bunch of argument days.
	"""
	
	dayset = set()
	
	for oneline in sys.stdin:
		dayset.add(oneline.strip())
			
	for oneday in dayset:	
		hadJavaCall.runHadoopCall("HadoopMode", [oneday])
