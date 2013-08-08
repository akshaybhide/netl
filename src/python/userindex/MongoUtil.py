#!/usr/bin/python

import re, os, sys, json

sys.path.append("/home/burfoot/src/python/shared")

import Util

MONGO_URL = "https://mongo-virginia1.adnetik.iponweb.net"

MONGO_USER = "adnetik"
MONGO_PASS = "Jshd73ea2"

def isValid(jso):
	
	return jso[u'total_rows'] > 0

def look4User(wtpid):
	
	for colid in range(3):
		jso = getJsonResponse(wtpid, colid)
		
		#print "Probe JSO is %s" % (jso)
		
		if isValid(jso):
			return jso
			
	return None
		
		
def getJsonResponse(wtpid, colid):
	
	syslist = getQueryResponse(wtpid, colid)
	
	return json.loads(" ".join(syslist))

def getQueryResponse(wtpid, colid):
	
	curlcall = "curl -s -u %s:%s -k -X GET %s/rtb/user_%d/?filter__id=%s" % (MONGO_USER, MONGO_PASS, MONGO_URL, colid, wtpid)

	#print "%s" % (curlcall)

	return Util.sysCallResult(curlcall)
	

if __name__ == "__main__":

	
	jso = look4User("aea3fce1-0bb0-4cbf-ac64-c037edf286c2")
	
	print "JSO is %s" % (jso)
