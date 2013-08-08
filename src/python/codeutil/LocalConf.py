
import os, pwd

# User login name
USER_NAME = (pwd.getpwuid(os.getuid())[0])

# This will set the home directory based on the user's information
USER_HOME = "/home/%s" % (USER_NAME)

PATH_TO_RSA = "/home/%s/.ssh/priv_key_342" % (USER_NAME)

def getJavaHomeDir():
	return "/usr/java/jdk1.6.0_21/bin"

def getSrcDirPath(isprod=False):
	
	return "%s/src" % (USER_HOME)
	
def getJavaSrcDir(isprod=False):
	
	return getSrcDirPath(isprod) + "/java"	
	
def getMainJarPath():
	mainjarpath = "%s/adnetik.jar" % (getJarDir(False))
	return 	mainjarpath
	
def getThirdPartyJarDir():
	return "%s/src/p3jars" % (USER_HOME)
	
def getJavaClassDir(isprod):

	return "%s/jclass" % (USER_HOME)
	
def getJarDir(isprod): 
	return "%s/jars" % (USER_HOME)
