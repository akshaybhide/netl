#!/usr/bin/python

import re, os, sys

TMP_PATH = "/tmp/burfoot/hadtest"
			
TARGET_PS = ["DataNode", "TaskTracker"]			
			
def getSubNodes():
	
	return [("anacluster0%d" % i) for i in range(2, 7)]
			
			
def jpsCheck(nodecode):
	
	jpsoutpath = "%s/jps_%s.txt" % (TMP_PATH, nodecode)
	
	jpscall = "ssh %s \"jps\" > %s" % (nodecode, jpsoutpath)
	
	#print jpscall
	os.system(jpscall)
	
	jpset = set()
	ccount = 0
	
	for line in open(jpsoutpath):
		(pid, jpsitem) = line.strip().split(" ")
		
		if jpsitem.strip() == "Child":
			ccount += 1
			continue
			
		if jpsitem.strip() == "Jps":
			continue
		
		jpset.add(jpsitem.strip())
	
	for targps in TARGET_PS:
		if not targps in jpset:
			print "\t\tERROR %s not running on %s" % (targps, nodecode)
			sys.exit(1)
	
	print "For node %s, found jobs: %s, \n\tNumber of child jobs is %d" % (nodecode, sorted(jpset), ccount)


if __name__ == "__main__":

	for nodecode in getSubNodes():
		jpsCheck(nodecode)
	
	
#danb
