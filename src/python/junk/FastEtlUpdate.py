#!/usr/bin/python

import sys, os, AnaBackFill
	
if __name__ == "__main__":

	locjarpath = "/mnt/jars/adnetik.jar"

	# Grab the jar file		
	AnaBackFill.grabJar()
	
	hadcall = "hadoop jar %s com.adnetik.slicerep.HadoopMode yest usebid=true" % (locjarpath)
	print "Hadoop call is %s" % (hadcall)
	os.system(hadcall)
	
	kvcall = "hadoop jar %s com.adnetik.slicerep.KvUploader yest" % (locjarpath)
	print "KV call is %s" % (kvcall)
	os.system(kvcall)
