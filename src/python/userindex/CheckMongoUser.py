#!/usr/bin/python

import re, os, sys, json

sys.path.append("/home/burfoot/src/python/shared")
sys.path.append("/home/burfoot/src/python/util")

import Util, MongoUtil

ALPHA_SET = set()

ALPHA_SET.add('a')
ALPHA_SET.add('b')
ALPHA_SET.add('c')
ALPHA_SET.add('d')
ALPHA_SET.add('e')
ALPHA_SET.add('f')


def isOkay(cookie):
	
	onechar = cookie[0]
	return onechar in ALPHA_SET

def hasPixId(jso, pixid):
	
	#return pixid in jso
	return pixid in jso[u'rows'][0]

def prettyPrint(jso):
	
	print json.dumps(jso, sort_keys=True, indent=4, separators=(',', ': '))

if __name__ == "__main__":

	ccount = 0
	cookiefile = sys.argv[1]
	
	checkid = sys.argv[2]
	
	cookset = set()
	
	for oneline in open(cookiefile):
		
		cookie = oneline.strip()
		
		if not isOkay(cookie):
			#print "Cookie is not okay %s" % (cookie)
			continue
			
		# print "Cookie is okay: %s" % (cookie)
		
		ccount += 1
		
		if ccount > 1000:
			break
			
		jso = MongoUtil.look4User(cookie)
		
		if jso == None:
			#print "No JSON found for cookie %s" % (cookie)
			continue
			
		if hasPixId(jso, checkid):
			print "Confirmed, cookie %s has checkid=%s" % (cookie, checkid)
		else:
			print "WARNING, cookie %s does not have checkid=%s" % (cookie, checkid)
			prettyPrint(jso)
						
