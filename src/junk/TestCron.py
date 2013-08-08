#!/usr/bin/python

###################################################
# Test that things are working even though 
# we're running as a cron job
###################################################

import os, sys, datetime
import SynchUtil


if __name__ == "__main__":

	excarg = "rtb"
	logarg = "no_bid"
	dayarg = "yest"
	
	# This is essential because we need to be able to write files
	# Mapred user must have access to this file
	os.chdir("/mnt/src/cronjobs")
	
	# demonstrate that we are running in the correct directory
	cwd_mssg = "TestCron running from directory  %s" % (os.getcwd())
	os.system("echo \"%s\" | wall" % cwd_mssg)

	# Okay, there's a lot of things we want to check.
	LOG_SYNC_CLASS = 'com.adnetik.data_management.TestCronOutput'
	hadoopsys = "hadoop jar %s %s > tcoutput.txt" % (SynchUtil.JAR_PATH, LOG_SYNC_CLASS) 
	# Does the output from this Java code go in the right place?
	os.system(hadoopsys)

	# Send the output of the Hadoop call to the system
	os.system("cat tcoutput.txt | wall")

	os.system("echo \"Test cron complete\" | wall")
	
	print "The time is now: %s" % (datetime.datetime.now())
	
	
	
	
