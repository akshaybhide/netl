#!/usr/bin/python

###################################################
# Revised version of log_sync
# 1) Generate s3 log file manifest
# 2) Run Java LogSync operation
# 3) Run LZO indexer
# 4) Clean up
###################################################

import os, sys
import SynchUtil, ConcatLzoSynch

JAR_PATH = '/mnt/javaclass/adnetik.jar'
UPDATE_TRACK_CLASS = 'com.adnetik.analytics.UpdateTrackFile'

def runTrackUpdate(daycode):
	
	hadoopsys = "hadoop jar %s %s %s" % (SynchUtil.JAR_PATH, UPDATE_TRACK_CLASS, daycode) 
			
	print "Hadoop call is %s" % ( hadoopsys )
	os.system(hadoopsys)		
	
if __name__ == "__main__":
	
	"""
	This is a one-time operation to copy the impression logs and update
	the tracking file
	
	"""

	exclist = SynchUtil.getCheckExcList('all')	
	daylist = SynchUtil.getCheckDayList(sys.argv[1])
	logtype = 'imp'

	for daycode in daylist:
		for onex in exclist:
			print "Uploading logs for %s %s %s" % (onex, logtype, daycode)
			ConcatLzoSynch.runLogSync(onex, logtype, daycode)
		
		ConcatLzoSynch.runIndexer()
				
		# Now we have indexed LZO files, so we can run Java UpdateTrackFile
		runTrackUpdate(daycode)
	
	

