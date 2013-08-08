#!/usr/bin/python

import re, os, sys, fileinput, JavaCall
			
def runHadoopCall(java_class, arglist, printonly=False, grabhost=None):
	
	full_class = JavaCall.getFullClass(java_class)
	tempjarpath = JavaCall.makeTempJarCopy(grabhost)

	had_call = "hadoop jar %s %s %s" % (tempjarpath, full_class, " ".join(arglist))

	print had_call
	
	if not printonly:
		os.system(had_call)
		
	# Remove the temp jar file
	os.remove(tempjarpath)		
			
if __name__ == "__main__":

	java_class = sys.argv[1]	
	runHadoopCall(java_class, sys.argv[2:])	
