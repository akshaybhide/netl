#!/usr/bin/python

import re, os, sys

TMP_PATH = "/tmp/burfoot/hadtest"
CONF_PATH = "/etc/hadoop-0.20/conf.oracles"
COPY_NODE = "anacluster02"
			
def getSubNodes():
	
	return [("anacluster0%d" % i) for i in range(3, 7)]
			
def getCheckFiles():
	
	return ["mapred-site.xml", "core-site.xml", "hdfs-site.xml"]
			
def copyOriginals(nodecode, filename):
	
	scpcall = "scp %s:%s/%s %s/orig-%s" % (nodecode, CONF_PATH, filename, TMP_PATH, filename)
	os.system(scpcall)	
		
def checkNodeConf(nodecode, filename):
	
	# Copy remote file 
	scpcall = "scp %s:%s/%s %s/copy-%s" % (nodecode, CONF_PATH, filename, TMP_PATH, filename)
	#print scpcall
	os.system(scpcall)
	
	diffpath = "%s/should_be_empty.txt" % (TMP_PATH)
	diffcall = "diff %s/orig-%s %s/copy-%s > %s" % (TMP_PATH, filename, TMP_PATH, filename, diffpath)
	os.system(diffcall)
	
	errlines = [eline for eline in open(diffpath)]
	
	if len(errlines) == 0:
		print "Check passed for %s, %s" % (nodecode, filename)
	else:
		print "ERROR not identical for %s %s" % (nodecode, filename)
		sys.exit(1)
			
if __name__ == "__main__":

	"""
	Split a file by tab, pick out a particular field as a key, then print the key-line.
	This is useful as a component of piped Hadoop jobs
	"""
	passcount = 0 
	
	for checkfile in getCheckFiles():
	
		copyOriginals(COPY_NODE, checkfile)
	
		for subnode in getSubNodes():
			checkNodeConf(subnode, checkfile)
			passcount += 1
	
	print "checked %d total files" % (passcount)
	print "\t%d file versions: %s" % (len(getCheckFiles()), getCheckFiles())
	print "\t%d sub nodes: %s" % (len(getSubNodes()), getSubNodes())

	
