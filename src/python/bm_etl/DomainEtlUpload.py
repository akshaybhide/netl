#!/usr/bin/python

import re, os, sys, fileinput, datetime
		
if __name__ == "__main__":

	"""
	Targ upload days
	"""

	domainlist = [line.strip() for line in open('domainupload.txt')]
	
	for daycode in domainlist:

		javacall = "hadJavaCall.py HdShard2Db %s > upload_log%s.txt" % (daycode, daycode)
		print "javacall is %s" % (javacall)
		os.system(javacall)
