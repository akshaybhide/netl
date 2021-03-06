#!/usr/bin/python

import re, os, sys, fileinput, LocalConf

# TODO remove this in favor of always using com.digilant
# Return map PackName-->SubPackage (com.adnetik or com.digilant)
def getPackNameMap():
	
	packmap = {}

	for onepack in getAdnetikPackList():
		packmap[onepack] = ["com", "adnetik", onepack]
		
	for onepack in getDigilantPackList():
		packmap[onepack] = ["com", "digilant", onepack]
		
	return packmap

def classList4Pack(simplepack):
	packdir = getSrcDir4Pack(simplepack)
	return [onefile.split(".")[0] for onefile in os.listdir(packdir) if onefile.endswith(".java")]

def srcPath4JavaFile(simplepack, simpleclass):
	return "%s/%s.java" % (getSrcDir4Pack(simplepack), simpleclass)

def getSrcDir4Pack(packname):
	
	packmap = getPackNameMap()
	
	assert packname in packmap, "Package name %s not found in packmap" % (packname)
	
	return "%s/%s" % (LocalConf.getJavaSrcDir(), "/".join(packmap[packname]))

def getPackageList():
	
	packlist = []
	packlist.extend(getAdnetikPackList())
	packlist.extend(getDigilantPackList())
	return packlist

def getDigilantPackList():
	
	packlist = []	
	packlist.append("fastetl")
	packlist.append("mobile")
	packlist.append("dbh")
	packlist.append("ntzetl")
	packlist.append("pixel")
	return packlist

def getAdnetikPackList():
	packlist = []
	packlist.append("shared")
	packlist.append("bm_etl")	
	packlist.append("data_management")	
	packlist.append("analytics")
	packlist.append("userindex")
	packlist.append("slicerep")	
	packlist.append("adhoc")	
	#packlist.append("hadtest")
	#packlist.append("pricing")	
	#packlist.append("fastetl")	
	
	return packlist

def getTargDirList(isprod):
	
	dirlist = []	
	packmap = getPackNameMap()
	
	for packname in getPackageList():
		
		# Hack for now, remove later
		if not 'adnetik' in packmap[packname]:
			continue
		
		srcdir = "/java/%s" % ("/".join(packmap[packname]))
		dirlist.append(srcdir)
		
	return dirlist
		
def explodeHelperJars(isprod):
	
	os.chdir(LocalConf.getJavaClassDir(isprod))
	
	for onejar in getJarFileList():
		if "mysql" in onejar:
			expcall = "jar -xf %s" % (onejar)			
			print "exploding Jar files: %s" % (expcall)
			os.system(expcall)		
		
def getSrcOrClassPath(packname, isprod, is_src):
	
	# TODO this is wrong
	basedir = LocalConf.getJavaSrcDir(isprod) if is_src else LocalConf.getJavaClassDir(isprod)
	interdir = "" if is_src else "com/adnetik/"
	return "%s/%s/%s" % (basedir, interdir, packname)

def getClassFilePath(packname, classname, isprod=False):
	
	packmap = getPackNameMap()
	assert packname in packmap
		
	filepath = "%s/%s/%s.class" % (LocalConf.getJavaClassDir(isprod), "/".join(packmap[packname]), classname)
	return filepath
		
def copyResourceFiles(isprod=False):
	
	srcdir = LocalConf.getSrcDirPath(isprod)
	classdir = LocalConf.getJavaClassDir(isprod)
	
	resdir = "%s/com/adnetik/resources" % (classdir)
	
	# Copy contents of resources directory to jar file root
	if not os.path.exists(resdir):
		os.system("mkdir " + resdir)
		
	rescall = "cp -r %s/resources/* %s/" % (srcdir, resdir)
	print rescall
	os.system(rescall)
	
def createJarFile(isprod=False):
	
	jardir = LocalConf.getJarDir(isprod)
	jclassdir = LocalConf.getJavaClassDir(isprod)
	jarcall = "jar -cf %s/adnetik.jar -C %s/ ." % (jardir, jclassdir)
	print(jarcall)
	os.system(jarcall)

def getJarFileList():
	
	jarfiles = []
	
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/hadoop-common.jar")
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/hadoop-annotations-2.0.0-cdh4.2.1.jar")
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/hadoop-common-2.0.0-cdh4.2.1.jar")
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/hadoop-mapreduce-client-core.jar")	
	
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/cloudera-hadoop-lzo-20110823162312.2bd0d5b.jar")
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/mysql.jar");
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/nzjdbc.jar");	
	
	#jarfiles.append(LocalConf.getThirdPartyJarDir() + "/postgres.jar");	
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/xercesImpl.jar");	 
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/xml-apis.jar");	
	jarfiles.append(LocalConf.getThirdPartyJarDir() + "/jsch-0.1.49.jar");	
	
	return jarfiles
	
def starBuildPack(packname, isprod=False):

	packmap = getPackNameMap()

	subpacklist = packmap[packname]
	#print "subpack list is %s" % (subpacklist)
	#print "Java src dir is %s" % (LocalConf.getJavaSrcDir(isprod))
	
	srcpath = "%s/%s" % (LocalConf.getJavaSrcDir(isprod), "/".join(packmap[packname]))
	
	#print "Source path is %s" % (srcpath)
	
	starBuild(srcpath, isprod)
	return srcpath
	
def starBuild(srcpath, isprod=False):
	"""
	Builds all the *.java files in srcdir to the given java class dir
	"""
	jclasspath = LocalConf.getJavaClassDir(isprod)
	classpathlist = getJarFileList()
	classpathlist.append(jclasspath)
	classpathstr = ":".join(classpathlist)
	
	xlintstr = "-Xlint:-deprecation" if "adnetik" in srcpath else ""
	
	syscall = "javac %s -Xlint:unchecked -classpath %s -d %s %s/*.java" % ( xlintstr, classpathstr, jclasspath, srcpath)
	print syscall
	os.system(syscall)	
	

if __name__ == "__main__":


	""" 
	1) Compile a list of files
	2) Rebuild LOCAL jar file
	""" 

	packlist = []

	if len(sys.argv) == 2:
		packlist = []
		#onetarg = "/java/%s/*.java" % (sys.argv[1])
		packlist.append(sys.argv[1])
	else:
		packlist = getPackageList()
	
	
	for onepack in packlist:
		
		builtpath = starBuildPack(onepack, False)
		print "Built files in directory %s" % (builtpath)
		
	copyResourceFiles(False)
		
	createJarFile(False)


