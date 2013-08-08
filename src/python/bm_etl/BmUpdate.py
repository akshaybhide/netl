#!/usr/bin/python

###################################################
# 1) Run Hadoop job
# 2) Call Java-Hadoop Hadoop2Infile program
###################################################

import os, sys

sys.path.append('/local/src/cronjobs')

import SynchUtil

LOG2BIN_CLASS = 'com.adnetik.bm_etl.NewLog2Bin'

# TODO: this is misnamed
def log2binCall(daycode, aggtype, logmail):
	
	hadcall = "hadoop jar %s %s %s usebid=true aggtype=%s" % (SynchUtil.JAR_PATH, LOG2BIN_CLASS, daycode, aggtype)
	logmail.addLogLine("Log2Bin call is %s" % (hadcall))	
	os.system(hadcall)
	

def had2infCall(daycode, h2inf_class, logmail):
	
	hadcall = "hadoop jar %s com.adnetik.bm_etl.%s %s" % (SynchUtil.JAR_PATH, h2inf_class, daycode)
	logmail.addLogLine("Had2inf call is %s" % (hadcall))
	os.system(hadcall)
			
	
if __name__ == "__main__":

	if not len(sys.argv) == 2:
		print "Usage:  BmUpdate <yest|daycode>"
		sys.exit(1)

	daylist = SynchUtil.getCheckDayList(sys.argv[1])

	# This is kind of the hacky way to do things
	sys.path.append("/local/src/python/util")
	import SimpleMail
	logmail = SimpleMail.SimpleMail("BM ETL Report")
	logmail.addLogLine("Starting daily BM ETL run")

	agglist = ['ad_general', 'ad_domain']
	h2inflist = ['Hadoop2Infile', 'HdShard2Db']

	#os.system(" echo \"running bmupdate\" | wall")


	for daycode in daylist:
				
		print "%s\n" % (SynchUtil.BIG_PRINT_BAR)	
		print "Running for daycode %s\n" % (daycode)
				
		log2binCall(daycode, "ad_general", logmail)
		logmail.addLogLine("Finished log2bin , now calling hadoop2infile..." )
						
		for h2inf in h2inflist:
			had2infCall(daycode, h2inf, logmail)
			logmail.addLogLine("Finished uploading to DB")
			
		print "Finished for daycode %s\n" % (daycode)			
		print "%s\n" % (SynchUtil.BIG_PRINT_BAR)	
							
			
	#logmail.send2admin()

