#!/usr/bin/python

import re, os, sys, fileinput
import CompJava, LocalConf

classPathDir = LocalConf.getJavaClassDir(False)
javaHomeDir = LocalConf.getJavaHomeDir()

def src2ClassPath(srcpath):
	
	classpath = srcpath.replace(javaHomeDir, classPathDir)
	classpath = classpath.replace("java", "class")
	return classpath

if __name__ == "__main__":

	targdirs = CompJava.getTargDirList(False)
	
	#print "Targ dirs is %s" % (targdirs)
	
	classPathData = []
	classPathData.append(classPathDir)
	classPathData.extend(CompJava.getJarFileList())
	
	classPathStr = ":".join(classPathData)

	ccount = 0

	for subtrg in targdirs:
				
		fulltrg = LocalConf.getSrcDirPath(False) + subtrg
				
		#print "Checking dir %s: " % subtrg
		
		packext = subtrg.split('/')[-1]
		
		#print "tdir is %s, Pack ext is %s" % (subtrg, packext)
		
		for jfile in os.listdir(fulltrg):
			
			if jfile.find('.') == -1:
				continue
			
			(classname, sbjava) = jfile.split('.')
			
			if not sbjava == 'java':
				continue
			
			javacode = "%s/%s" % (fulltrg, jfile)
			classpathfile = CompJava.getClassFilePath(packext, classname, False)
		
			#print "java path is %s, classpath is %s" % (javacode, classpathfile)
			#sys.exit(1)
		
			javamod = os.path.getmtime(javacode)
			
			if os.path.exists(classpathfile):		
				clssmod = os.path.getmtime(classpathfile)
			else:
				clssmod = 0
			
			if javamod > clssmod :
				
				print "Recompiling %s" % (javacode)

				xlintstr = "-Xlint:deprecation -Xlint:unchecked" if "adnetik" in fulltrg else ""

				syscall = "javac %s -classpath %s -d %s %s" % ( xlintstr, classPathStr, classPathDir, javacode)
				print syscall
				os.system(syscall)	
				ccount += 1

	print "Recompiled %d files" % (ccount)
	
	CompJava.copyResourceFiles(False)
	CompJava.createJarFile(False)

