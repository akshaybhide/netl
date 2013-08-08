#!/usr/bin/python

###################################################
# 1) Check if no Hadoop jobs are running. 
# 2) If so, wait. 
# 3) If not, find a missing LZO file, and do the update.
# 4) Go back to 1
###################################################

import os, time, sys, datetime, random, traceback
import SynchUtil, ConcatLzoSynch

# This is kind of the hacky way to do things
sys.path.append("/mnt/src/python/util")
import SimpleMail

FILE_COUNT_WARN_CUTOFF = 5

def noDataOkaySet():
	
	okset = set()
	okset.add("nexage")
	okset.add("dbh")
	return okset	

def checkExchangeDirs(logmail, logtype, daycode):
	
	hadcall = "hadoop fs -ls /data/%s/%s/" % (logtype, daycode)
	hadlines = SynchUtil.sysCallResult(hadcall)
	warncount = 0
	
	for excname in SynchUtil.getExchanges():
				
		if excname in noDataOkaySet():
			continue
		
		dirpath = "/data/%s/%s/%s" % (logtype, daycode, excname)
		
		if not any(dirpath in hline for hline in hadlines):
			logmail.addLogLine("Warning: directory not found: %s" % (dirpath))
			warncount += 1
			continue # no point in continuing
			
			
		subhadls = "hadoop fs -ls %s" % (dirpath)
		subfiles = SynchUtil.sysCallResult(subhadls)
		
		if len(subfiles) < FILE_COUNT_WARN_CUTOFF:
			logmail.addLogLine("Warning: found only %d files for exchange %s" % (len(subfiles), excname))
			warncount += 1
			
	return warncount
		

if __name__ == "__main__":

	"""
	Check to make sure that the daily Synch Jobs ran correctly. 
	If they didn't, send an AdminMail
	"""
	
	daycode = SynchUtil.get_yesterday() if "yest" in sys.argv[1] else sys.argv[1]
	
	logmail = SimpleMail.SimpleMail("File Synch Check for %s" % (daycode))	

	warncount = 0
	warncount += checkExchangeDirs(logmail, "no_bid", daycode)
	warncount += checkExchangeDirs(logmail, "bid_all", daycode)
	
	if warncount > 0:
		logmail.send2admin()
			
	

