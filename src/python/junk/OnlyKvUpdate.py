#!/usr/bin/python

import sys, os, AnaBackFill
	
if __name__ == "__main__":

	locjarpath = "/mnt/jars/adnetik.jar"

	daycode = sys.argv[1]	

	# Grab the jar file		
	#AnaBackFill.grabJar()
	
	kvcall = "hadoop jar %s com.adnetik.slicerep.KvUploader %s" % (locjarpath, daycode)
	print "KV call is %s" % (kvcall)
	os.system(kvcall)
