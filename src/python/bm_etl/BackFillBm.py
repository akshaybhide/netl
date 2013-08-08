#!/usr/bin/python

import re, os, sys, fileinput, datetime
		
if __name__ == "__main__":

	"""
	Just cut off the first element of a line
	"""
	
	tdlist = [line.strip() for line in open('targdays.txt')]

	print "Found %d target days, first is %s, last is %s" % (len(tdlist), tdlist[0], tdlist[-1])

	for daycode in tdlist:
		
		logfile = "etl_backfill_%s.log" % (daycode)
		errfile = "etl_backfill_%s.err" % (daycode)

		l2bcall = "hadoop jar /local/bin/jars/adnetik.jar com.adnetik.bm_etl.NewLog2Bin %s >> %s 2>> %s" % (daycode, logfile, errfile)
		print "l2bcall is %s" % (l2bcall)
		os.system(l2bcall)

		h2icall = "hadoop jar /local/bin/jars/adnetik.jar com.adnetik.bm_etl.Hadoop2Infile %s >> %s 2>> %s" % (daycode, logfile, errfile)
		print "h2icall is %s" % (h2icall)
		os.system(h2icall)
