#!/usr/bin/python

###################################################
# Smarter version of GetBucket that can be used to 
# generate manifests for multiple days
###################################################

import os, sys
import SynchUtil

def addOneMani(exchange, logtype, daycode, usepref):
	
	SynchUtil.writeManiFile(exchange, logtype, daycode, usepref)
	
	manipath = SynchUtil.getManiPath(exchange, logtype, daycode)
	
	for line in open(manipath):
		print line,
		
	#os.remove(manipath)
	

if __name__ == "__main__":
	
	exclist = SynchUtil.getCheckExcList(sys.argv[1])	
	loglist = SynchUtil.getCheckLogList(sys.argv[2])
	daylist = SynchUtil.getCheckDayList(sys.argv[3])
	
	usepref = ""
	
	if len(sys.argv) == 5:
		usepref = sys.argv[4]
		
	if usepref == 's3npref':
		usepref = SynchUtil.S3N_PREF
	
	for onex in exclist:
		for logtype in loglist:
			for daycode in daylist:
				addOneMani(onex, logtype, daycode, usepref)
	
	
	
	
