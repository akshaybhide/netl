#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	"""
	Print out environment variables
	"""
	
	for envkey in os.environ:
		print "%s ==> %s" % (envkey, os.environ[envkey])
