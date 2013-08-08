#!/usr/bin/python

import sys, os, GrabJar 

if __name__ == "__main__":
	
	# Grab the jar file		
	GrabJar.grabJar()
	
	jarlist = []
	jarlist.append("/home/burfoot/jars/hadoop-core.jar")
	jarlist.append(GrabJar.LOCAL_JAR_PATH)
	
	jvcall = "java -Xmx20G -cp %s com.adnetik.slicerep.BatchRunner" % (":".join(jarlist))
	
	os.system(jvcall)
