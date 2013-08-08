#!/usr/bin/python

import re, os, sys

TMP_PATH = "/tmp/burfoot/hadtest"
			
def getSubNodes():
	
	return [("anacluster0%d" % i) for i in range(2, 7)]
			
			
def checkDiskUse(nodecode, hdd):
	
	hddfull = (("0%d" if hdd < 10 else "%d") % hdd)
	
	dfoutput = "%s/node%s_disk%s.txt" % (TMP_PATH, nodecode, hddfull)
		
	dfcall = "ssh %s \"df /mnt/hdd/%s\" > %s" % (nodecode, hddfull, dfoutput)
	#print dfcall
	os.system(dfcall)
	
	dflines = [line for line in open(dfoutput)]
	
	reltoks = dflines[1].split(" ")
	perctok = reltoks[-2]

	print "Percentage use for %s, disk %s is %s" % (nodecode, hddfull, perctok)
	
	if perctok > "90%":
		print "\t\tWARNING: disk too full %s" % (perctok)
		sys.exit(1)

	return perctok

if __name__ == "__main__":

	passcount = 0 
	percset = set()
	
	for disknum in range(2, 13):
		for subnode in getSubNodes():
			percuse = checkDiskUse(subnode, disknum)
			percset.add(percuse)
			
	perclist = sorted([perc for perc in percset])
	print "Found percentages: %s" % (perclist)

	
