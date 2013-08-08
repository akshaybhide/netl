#!/usr/bin/python

###################################################
# 1) Read from a list of <
###################################################

import os, sys
import ConcatLzoSynch, SynchUtil
	
if __name__ == "__main__":

	# need to make sure we're running in a folder where we have write permissions,
	# otherwise we won't be able to write the manifest file
	os.chdir('/mnt/src/cronjobs/')

	exclist = [] 
	daylist = []
	loglist = []

	for line in sys.stdin:

		if len(line.strip().split('\t')) < 3:
			continue
		
		(excCode, logType, dayCode) = line.strip().split('\t')
		
		#print "Syncing %s %s %s" % (excCode, logType, dayCode)
		
		exclist.append(SynchUtil.getCheckExcList(excCode)[0])
		loglist.append(SynchUtil.getCheckLogList(logType)[0])
		daylist.append(SynchUtil.getCheckDayList(dayCode)[0])		
		
		
	idxlist = []	
		
	for i in range(len(exclist)):
		print "Syncing %s %s %s" % (exclist[i], loglist[i], daylist[i])
		ConcatLzoSynch.runLogSync(exclist[i], loglist[i], daylist[i])
		idxlist.add(SynchUtil.getHdfsPath(exclist[i], loglist[i], daylist[i]))

	for toidx in idxlist: 
		ConcatLzoSynch.runIndexer(toidx)

	

