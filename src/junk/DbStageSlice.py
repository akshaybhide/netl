#!/usr/bin/python

###################################################
# Just a wrapper for the two SliceInterest jobs
# Want these to run back to back, hard to do without a wrapper
###################################################

import os, sys
import SynchUtil

LOG_MAIL_TEMP = "log_mail_tmp.txt"


def runCall(dmclass, daycode, tempfile):
	
	# TODO: change this to use LocalConf
	hadcall = "hadoop jar /local/bin/jars/adnetik.jar %s %s %s" % (dmclass, daycode, tempfile)
	print "\nHadoop call is : \n\t%s" % (hadcall)
	
	os.system(hadcall)
	
	#print "\nFinished with %s" % (dmclass)
	
if __name__ == "__main__":

	if not len(sys.argv) == 2:
		print "Usage:  BmUpdate <yest|daycode>"
		sys.exit(1)

	daylist = SynchUtil.getCheckDayList(sys.argv[1])
	daycode = daylist[0]

	# This is kind of the hacky way to do things
	sys.path.append("/local/src/python/util")
	import SimpleMail
	logmail = SimpleMail.SimpleMail("DbStageSlice")

	javalist = []
	
	# TODO: going to roll all of this into a single Java file StagingInfoManager, obviate this Python script
	#javalist.append("com.adnetik.data_management.Special2Staging")	
	javalist.append("com.adnetik.userindex.StagingInfoManager")
	#javalist.append("com.adnetik.data_management.Click2Staging")
	#javalist.append("com.adnetik.data_management.Negative2Staging")
	
	for onejava in javalist:
		
		tempfile = "log_mail_temp.txt"
		open(tempfile, 'w').write('Setting up for %s' % onejava)
		
		runCall(onejava, daycode, tempfile)

		# Slurp the log data, then delete tempfile
		logmail.readLogData(tempfile)
		os.remove(tempfile)
	
	#logmail.send2admin()
	
	# Don't run this as cron job anymore 
	# run the LocalMode job separately
	# locmode = "hadoop jar /local/bin/jars/adnetik.jar com.adnetik.userindex.LocalMode yest force=true"
	# os.system(locmode)


	
