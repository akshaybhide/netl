#!/usr/bin/python

import re, os, sys, fileinput, CompJava, LocalConf

from random import randint
			
JAVA_CLASS_DIR = LocalConf.getJavaClassDir(False)		

def getTempJarPath(pid):
	
	return "%s/tempjar_%d.jar" % (LocalConf.getJarDir(False), pid)

def makeTempJarCopy(grabhost=None):

	pid = randint(0, 10000000)
	
	while(os.path.exists(getTempJarPath(pid))):
		pid = randint(0, 10000000)

	tempjarpath = getTempJarPath(pid)
	mainjarpath = LocalConf.getMainJarPath()
	
	if not grabhost == None:
		copycall = "scp -i %s %s@%s:%s %s" % (LocalConf.PATH_TO_RSA, LocalConf.USER_NAME, grabhost, mainjarpath, tempjarpath)
        else:
        	copycall = "cp %s %s" % (mainjarpath, tempjarpath)
		
	print "Grab call is %s" % (copycall)
	os.system(copycall)
	return tempjarpath

def getFullClass(java_class):
	
	clPackMap = buildClassPackMap()

	if java_class in clPackMap:
		pack_name = ".".join(clPackMap[java_class])
		print "found %s in package %s" % (java_class, pack_name)
		full_class = "%s.%s" % (pack_name, java_class)
		return full_class
	else:
		print "Error: class %s not found in class path" % (java_class)
		sys.exit(1)		
		
def buildClassPackMap():

	cl_pack_map = {}
	
	for root, dirs, files in os.walk(JAVA_CLASS_DIR):

		isadnetik = "adnetik" in root
		isdigilant = "digilant" in root

		if not ("adnetik" in root or "digilant" in root):
			continue
			
		cl_list = [f.split(".")[0] for f in files if f.find(".class") > -1]
		
		# TODO: this sucks, and breaks when you change configuration
		path_list = root.split("/")
		
		assert path_list[1] == "home"
		#assert path_list[2] == "burfoot"
		assert path_list[3] == "jclass"
		
		if len(path_list) >= 6:

			path_list = path_list[4:]
	
			for one_cl in cl_list:
				if one_cl in cl_pack_map and isadnetik:
					print "Warning: found %s in package %s" % (one_cl, cl_pack_map[one_cl])
				else:
					cl_pack_map[one_cl] = path_list
			
	return cl_pack_map
				

def runCall(java_class, arglist, doactualcall=True, grabhost=None):
	
	full_class = getFullClass(java_class)	
	
	tempjarpath = makeTempJarCopy(grabhost)
	print "Copied to temp jar path %s" % (tempjarpath)
	
	classpathlist = CompJava.getJarFileList()
	classpathlist.append(tempjarpath)
	classpathstr = ":".join(classpathlist)
		
	java_call = "java -cp %s %s %s" % (classpathstr, full_class, " ".join(arglist))

	print java_call
	
	# Use doactual=false for just printing out the call
	if doactualcall:
		os.system(java_call)
	
	# Remove the temp jar file
	os.remove(tempjarpath)
	
	
if __name__ == "__main__":
	
	java_class = sys.argv[1]	
	runCall(sys.argv[1], sys.argv[2:])
	
