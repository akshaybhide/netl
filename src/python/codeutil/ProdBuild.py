#!/usr/bin/python

import re, os, sys, fileinput, LocalConf, CompJava
	
localClassDir = LocalConf.getJavaClassDir(True)	
	
def deleteClassFiles():

	delcall = "rm -rf %s" % (localClassDir)
	print "Delete call is %s" % (delcall)
	os.system(delcall)
	
	os.system("mkdir %s" % (localClassDir))


def checkJava2ClassMap(packname, isprod):
	
	srcdir = CompJava.getSrcDir4Pack(packname)
	missing = []
	jcount = 0
	mcount = 0
	
	for onejava in os.listdir(srcdir):
		if ".java" in onejava:
			cname = onejava.split(".")[0]
			classfilepath = CompJava.getClassFilePath(packname, cname, isprod)
			jcount += 1
			
			if not os.path.exists(classfilepath):
				print "Could not find class file path %s for %s" % (classfilepath, cname)
				mcount += 1
			
	if jcount == 0:
		print "Error: no java files found for package %s" % (packname)
		sys.exit(1)
		
	if mcount > 0:
		print "Build error for package %s" % (packname)
		sys.exit(1)
		
	print "Build successful, found %d class files for package %s" % (jcount, packname)	


if __name__ == "__main__":

	deleteClassFiles()	
	CompJava.explodeHelperJars(True)
	
	pcount = 0
	
	for onepack in CompJava.getPackageList():
		CompJava.starBuildPack(onepack, True)
		#checkJava2ClassMap(onepack, True)
		#print "Built package %s" % (onepack)
		pcount += 1
		
	#CompJava.copyResourceFiles(True)
	#CompJava.createJarFile(True)
