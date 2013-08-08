#!/usr/bin/python

###################################################
# Just 
###################################################

import os, sys, pwd

# This will set the home directory based on the user's information
USER_HOME = "/home/%s" % (pwd.getpwuid(os.getuid())[0])

sys.path.append("%s/src/python/codeutil" % (USER_HOME))

import hadJavaCall,SynchUtil

LZO_JAR_PATH = '/usr/lib/hadoop-0.20/lib/hadoop-lzo-0.4.15.jar'
LZO_CLASS = 'com.hadoop.compression.lzo.DistributedLzoIndexer'
LOCAL_INDEXER_CLASS = 'com.hadoop.compression.lzo.LzoIndexer'

LZO_INDEX_LOGDIR = "/var/log/cronlogs/hdfs/lzoindexer"

FINDER_CLASS = "LzoIndexTargetFinder"

LZO_PATTERN = "/data/*/*.lzo"


def logFailureList():
	
	todaycode = SynchUtil.get_today()
	
	failpath = "/var/log/cronlogs/hdfs/lzoindexer/nolzolist_%s.txt" % (todaycode)
	
	hadJavaCall.runHadoopCall(FINDER_CLASS, [LZO_PATTERN, failpath])

def runLzoPass():
		
	noindexfile = "%s/noindexfile.txt.tmp" % (USER_HOME)
		
	hadJavaCall.runHadoopCall(FINDER_CLASS, [LZO_PATTERN, noindexfile])

	for onehdfspath in open(noindexfile):
		
		lochadcall = "hadoop jar %s %s %s" % (LZO_JAR_PATH, LOCAL_INDEXER_CLASS, onehdfspath)
		
		print "Local had call is %s" % (lochadcall)
			
		exitcall = os.system(lochadcall)



if __name__ == "__main__":


	runLzoPass()
	
	logFailureList()
	

