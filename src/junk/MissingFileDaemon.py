#!/usr/bin/python

###################################################
# 1) Check if no Hadoop jobs are running. 
# 2) If so, wait. 
# 3) If not, find a missing LZO file, and do the update.
# 4) Go back to 1
###################################################

import os, time, sys, datetime, random, traceback
import SynchUtil, ConcatLzoSynch

MISSING_FILE_CLASS = "com.adnetik.data_management.MissingFileCheck"

def countHadoopJobs():
	
	hadjobcall = "hadoop job -list > hadjoblist.txt";
	os.system(hadjobcall)
	
	hlines = [line for line in open('hadjoblist.txt')]
	
	# eg:
	#0 jobs currently running
	#JobId   State   StartTime       UserName        Priority        SchedulingInfo
	return len(hlines) - 2	
	
def getMissingFile():
	
	mfcall = "hadoop jar %s %s > missingfilelist.txt" % (SynchUtil.JAR_PATH, MISSING_FILE_CLASS)	
	os.system(mfcall)
	
	return [line.strip() for line in open('missingfilelist.txt') if len(line.strip()) > 0]
	
def getNonNexageMissing():
	
	return [line for line in getMissingFile() if line.find("nexage") == -1]
	
if __name__ == "__main__":

	
	while True:
		jobcount = countHadoopJobs()
	
		if jobcount > 0:
			rightnow = datetime.datetime.now()
			print "Found %d jobs running at %s, sleeping... " % (jobcount, rightnow.strftime("%Y-%m-%d %H:%M:%S"))
			time.sleep(5)
			
		else:
			loglines = getNonNexageMissing()
			
			if len(loglines) == 0:
				print "All desired files are present...! exiting"
				sys.exit(1)
			
			# Shuffle so that we don't repeatedly call on a single line,
			# if there's a problem with that line
			random.shuffle(loglines)
			toks = loglines[0].split("\t")			
			
			try:


	
				if len(toks) == 3:
					(excname, logtype, daycode) = loglines[0].split("\t")		
					print "Running ConLzoSync for %s, %s, %s" % (excname, logtype, daycode)		
					ConcatLzoSynch.runLogSync(excname, logtype, daycode)
				else:
					# assert toks[0] == "pixel"
					daycode = toks[1]
					print "Running PixLogSync for %s" % (daycode)
					hadcall = "hadoop jar %s %s %s" % (SynchUtil.JAR_PATH, SynchUtil.PIX_LOG_SYNC_CLASS, daycode)
					print hadcall
					os.system(hadcall)
			except:
				print "error found for line %s" % (loglines[0])
				exc_type, exc_value, exc_traceback = sys.exc_info()
				print "*** print_tb:"
				traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
				print "*** print_exception:"
				traceback.print_exception(exc_type, exc_value, exc_traceback, limit=2, file=sys.stdout)
				print "*** print_exc:"
				traceback.print_exc()
				

