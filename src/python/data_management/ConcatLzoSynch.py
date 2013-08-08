#!/usr/bin/python

###################################################
# Revised version of log_sync
# 1) Generate s3 log file manifest
# 2) Run Java LogSync operation
# 3) Run LZO indexer
# 4) Clean up
###################################################

import os, sys
import SynchUtil, RunIndexer

JAR_PATH = '/local/bin/jars/adnetik.jar'
LOG_SYNC_CLASS = 'com.adnetik.data_management.LogSync'

#/usr/lib/hadoop-0.20/lib/cloudera-hadoop-lzo-20111031101103.8aa0605-1.jar

LZO_JAR_PATH = '/usr/lib/hadoop-0.20/lib/cloudera-hadoop-lzo-20111031101103.8aa0605-1.jar'
LZO_CLASS = 'com.hadoop.compression.lzo.DistributedLzoIndexer'


def callHadoop(exchange, logtype, daycode):
	
	#manifilepath = SynchUtil.getManiPath(exchange, logtype, daycode)

	hadoopsys = "hadoop jar %s %s %s %s %s" % (JAR_PATH, LOG_SYNC_CLASS, exchange, logtype, daycode) 
	#hadoopsys = "hadoop jar %s %s -conf /mnt/data/burfoot/hadoop-localhdfs.xml %s" % (JAR_PATH, LOG_SYNC_CLASS, manifilepath) 
		
	print "Hadoop call is %s" % ( hadoopsys )
	os.system(hadoopsys)		

def runLogSync(exchange, logtype, daycode):

	# Got rid of all the Mani-file nonsense
	#SynchUtil.writeManiFile(exchange, logtype, daycode, writesize=True)
	callHadoop(exchange, logtype, daycode)
		
if __name__ == "__main__":

	if not len(sys.argv) == 4:
		print "Usage: ConcatLzoSynch <all|adex> <big|mini|comp|logtype> <yest|daycode>"
		sys.exit(1)

	# need to make sure we're running in a folder where we have write permissions,
	# otherwise we won't be able to write the manifest file
	os.chdir('/local/src/cronjobs/')

	exclist = SynchUtil.getCheckExcList(sys.argv[1])	
	loglist = SynchUtil.getCheckLogList(sys.argv[2])
	daylist = SynchUtil.getCheckDayList(sys.argv[3])

	idxlist = []

	for onex in exclist:
		for logtype in loglist:
			for daycode in daylist:
				
				# This is now done by Java code
				##if not SynchUtil.nfsFilesExist(onex, logtype, daycode):
				#	print "No NFS files for %s %s %s" % (onex, logtype, daycode)
				#	continue
				
				print "Running ConcatLogSynch for %s %s %s" % (onex, logtype, daycode)
				runLogSync(onex, logtype, daycode)
				
				idxlist.append(SynchUtil.getHdfsPath(onex, logtype, daycode))
		
	for toidx in idxlist:
		pass
		#RunIndexer.runIndexer(toidx)

	

