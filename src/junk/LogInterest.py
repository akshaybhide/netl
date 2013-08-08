#!/usr/bin/python

###################################################
# Wrapper for LogInterestActivity jobs
# Want these to run back to back, hard to do without a wrapper
###################################################

import os, sys
import SynchUtil

def runCall(dmclass, daycode):
	
	hadcall = "hadoop jar /mnt/jars/adnetik.jar com.adnetik.data_management.%s %s" % (dmclass, daycode)
	print "\nHadoop call is : \n\t%s" % (hadcall)
	
	os.system(hadcall)
	
	print "\nFinished with %s" % (dmclass)
	
if __name__ == "__main__":

	# this is ALWAYS going to run using "yesterday"

	daycode = SynchUtil.get_yesterday()

	runCall("LogInterestActivity", daycode)
	runCall("DailyNegativeUserScan", daycode)

	
