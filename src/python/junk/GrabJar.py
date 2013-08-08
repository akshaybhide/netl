#!/usr/bin/python

import sys, os

PATH_TO_RSA = "/home/burfoot/.ssh/priv_key_342"
PATH_TO_JAR = "/home/burfoot/jars/adnetik.jar"
LOCAL_JAR_PATH = "/home/burfoot/jars/adnetik.jar"

def grabJar():
	
	grabcall = "sftp -i %s burfoot@gandalf.adnetik.com:%s %s" % (PATH_TO_RSA, PATH_TO_JAR, LOCAL_JAR_PATH)
	print "Grab call is %s" % (grabcall)
	os.system(grabcall)
	
if __name__ == "__main__":

	grabJar()
