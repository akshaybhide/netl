#!/usr/bin/python

import re, os, sys, fileinput, datetime
		
if __name__ == "__main__":

	"""
	Just cut off the first element of a line
	"""

	domainlist = [line.strip() for line in open('domaintarg.txt')]

	for daycode in domainlist:
		
		javacall = "hadJavaCall.py NewLog2Bin %s aggtype=ad_domain > domain_backfill%s.txt" % (daycode, daycode)
		print "javacall is %s" % (javacall)
		os.system(javacall)
