#!/usr/bin/python

###################################################
# Just a wrapper for the two SliceInterest jobs
# Want these to run back to back, hard to do without a wrapper
###################################################

import os, sys
import SynchUtil

LOG_MAIL_TEMP = "log_mail_tmp.txt"


def runCall(dmclass, daycode, tempfile):
	
	# TODO: change this
	hadcall = "hadoop jar /local/bin/jars/adnetik.jar %s %s %s" % (dmclass, daycode, tempfile)
	print "\nHadoop call is : \n\t%s" % (hadcall)
	
	os.system(hadcall)
	
	#print "\nFinished with %s" % (dmclass)
	
if __name__ == "__main__":

	daycode = "yest"

	# This is kind of the hacky way to do things
	sys.path.append("/local/src/python/util")
	import SimpleMail
	logmail = SimpleMail.SimpleMail("DbStageSlice")

	javalist = []
	javalist.append("com.adnetik.data_management.Special2Staging")	
	javalist.append("com.adnetik.data_management.Pixel2Staging")
	javalist.append("com.adnetik.data_management.Click2Staging")
	javalist.append("com.adnetik.data_management.Negative2Staging")
	
	for onejava in javalist:
		
		tempfile = "log_mail_temp.txt"
		open(tempfile, 'w').write('Setting up for %s' % onejava)
		
		runCall(onejava, daycode, tempfile)

		# Slurp the log data, then delete tempfile
		logmail.readLogData(tempfile)
		os.remove(tempfile)
		
	
	logmail.send2admin()


	
