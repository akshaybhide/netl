#!/usr/bin/python

###################################################
# Wrapper for InterestUpdate code
###################################################

import os, sys
import SynchUtil

INT_UPDATE_CLASS = 'com.adnetik.analytics.InterestUserUpdate'


def callHadoop(exchange, logtype, daycode):
	
	manifilepath = SynchUtil.getManiPath(exchange, logtype, daycode)

	hadoopsys = "hadoop jar %s %s %s" % (SynchUtil.JAR_PATH, INT_UPDATE_CLASS, manifilepath) 
			
	#print "Hadoop call is %s" % ( hadoopsys )
	os.system(hadoopsys)	


def runIntUpdate(exchange, logtype, daycode):

	SynchUtil.writeManiFile(exchange, logtype, daycode, writesize=True)
	callHadoop(exchange, logtype, daycode)
	
	# delete manifest file
	manipath = SynchUtil.getManiPath(exchange, logtype, daycode)
	locRmCall = "rm %s" % (manipath)
	#print "Local rm call is %s" % (locRmCall)
	os.system(locRmCall)	
	

if __name__ == "__main__":
	
	if not len(sys.argv) == 4:
		print "Usage: InterestUpdateWrapper <all|adex> <big|mini|comp|logtype> <yest|daycode|filename>"
		sys.exit(1)

	# need to make sure we're running in a folder where we have write permissions,
	# otherwise we won't be able to write the manifest file
	os.chdir('/mnt/src/cronjobs/')	
	
	exclist = SynchUtil.getCheckExcList(sys.argv[1])	
	loglist = SynchUtil.getCheckLogList(sys.argv[2])
	daylist = SynchUtil.getCheckDayList(sys.argv[3])
	
	for onex in exclist:
		for logtype in loglist:
			for daycode in daylist:
				runIntUpdate(onex, logtype, daycode)
	
	
	
	
