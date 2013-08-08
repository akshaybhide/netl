#!/usr/bin/env python


import sys


curkey = None
curset = set()

def printIfAlive():
	
	if curkey:
		print "%s\t%s" % ( curkey, ",".join(curset) )
		
		
for line in sys.stdin:
	(newkey, an_item) = line.strip().split('\t')
   		
	if newkey != curkey:
		
		printIfAlive()
		
		curkey = newkey
		curset = set()
   
   	curset.add(an_item)

# Need to print final curval
printIfAlive()
