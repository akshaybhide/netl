#!/usr/bin/python

###################################################
# 1) Check if no Hadoop jobs are running. 
# 2) If so, wait. 
# 3) If not, find a missing LZO file, and do the update.
# 4) Go back to 1
###################################################

import os, time, sys, datetime, random
import MissingFileDaemon, SynchUtil

ACTIVITY_CLASS = 'com.adnetik.data_management.LogInterestActivity'

if __name__ == "__main__":

	daylist = []

	for line in sys.stdin:
		daylist.append(line.strip())
	
	dyi = 0
	
	while dyi < len(daylist):
		
		jobcount = MissingFileDaemon.countHadoopJobs()
	
		if jobcount > 0:
			rightnow = datetime.datetime.now()
			print "Found %d jobs running at %s, sleeping... " % (jobcount, rightnow.strftime("%Y-%m-%d %H:%M:%S"))
			time.sleep(5)
			
		else:
			daycode = daylist[dyi]
			hadcall = "hadoop jar %s %s %s" % (SynchUtil.JAR_PATH, ACTIVITY_CLASS, daycode)
			print "Hadoop Call is %s" % ( hadcall )
			os.system(hadcall)
			dyi += 1
		
