#!/usr/bin/python

import re, os, sys, fileinput

SHADOW_SRC = "/tmp/shadowsrc"

if __name__ == "__main__":

	packagelist = []
	packagelist.append('shared')
	packagelist.append('userindex')
	packagelist.append('hadtest')
	packagelist.append('analytics')
	packagelist.append('data_management')
	packagelist.append('adhoc')
	
	
	
	classPathData = []
	#classPathData.append("/usr/lib/hadoop-0.20/lib/core-3.1.1.jar")
	classPathData.append("/usr/lib/hadoop-0.20/hadoop-core.jar")
	classPathData.append("/usr/lib/hadoop-0.20/lib/hadoop-lzo-0.4.9.jar")
	classPathData.append("/mnt/jars/cloudera-hadoop-lzo-20110823162312.2bd0d5b.jar")
	classPathData.append("/mnt/jars/mysql.jar")	
	classPathStr = ":".join(classPathData)
	
	# copy all the files to shadowsrc directory
	for onepack in packagelist:
		
		# delete old stuff
		delcall = "rm %s/com/adnetik/%s/*.java" % (SHADOW_SRC, onepack)
		#print delcall
		os.system(delcall)
		
		packpath = "%s/com/adnetik/%s/" % (SHADOW_SRC, onepack)
		if not os.path.exists(packpath):
			os.makedirs(packpath)
		
		# copy 
		cpcall = "cp /mnt/src/java/%s/*.java %s/com/adnetik/%s/" % (onepack, SHADOW_SRC, onepack)
		os.system(cpcall)
		
	packstr = " ".join(["com.adnetik." + packname for packname in packagelist])

	jdoc_call = "javadoc -private -d /tmp/javadoc -classpath %s -sourcepath %s %s" % (classPathStr, SHADOW_SRC, packstr)
	print jdoc_call
	os.system(jdoc_call)
	
	
	
	#os.system(jdoc_call)
