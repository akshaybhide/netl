#!/usr/bin/python

import re, os, sys, fileinput, JavaCall
									
if __name__ == "__main__":

	java_class = sys.argv[1]
	
	full_class = JavaCall.getFullClass(java_class)
		
	had_call = "hadoop %s -conf /home/burfoot/hadoop-localhdfs.xml %s" % (full_class, " ".join(sys.argv[2:]))

	print had_call
	os.system(had_call)
	
