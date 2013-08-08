#!/usr/bin/python

###################################################
# 1) generate a manifest file
# 2) put on Hadoop 
# 3) hadoop distcp using a system call
# 4) remove mani file
###################################################

import os, sys
import SynchUtil

def doSimpleSynch(adex, logtype, daycode):
	
	print "Running SimpleSynch for adex=%s, logtype=%s, daycode=%s" % (adex, logtype, daycode)
	
	# Generate a manifest file, using appropriate prefix
	manipath = SynchUtil.writeManiFile(adex, logtype, daycode)
	hdfsMani = "/tmp/mani/%s" % (manipath)
	
	# Put the manifest file on HDFS
	putCall = "hadoop fs -put %s %s" % (manipath, hdfsMani)
	print "PutCall: %s" % (hdfsMani)
	os.system(putCall)
	
	# run distcp
	hdfsDir = "/data/%s/%s/%s/" % (logtype, daycode, adex)	
	distCpCall = "hadoop distcp -f %s %s" % (hdfsMani, hdfsDir)
	print "DistCpCall: %s" % (distCpCall)
	os.system(distCpCall)
	
	# delete mani file
	hdfsRmCall = "hadoop fs -rm %s" % (hdfsMani)
	print hdfsRmCall
	os.system(hdfsRmCall)
	
	# Delete local file
	locRmCall = "rm %s" % (manipath)
	print "Local rm call is %s" % (manipath)
	os.system(locRmCall)
	
	
if __name__ == "__main__":

	if not len(sys.argv) == 4:
		print "Usage: SimpleSynch <all|adex> <big|mini|comp|logtype> <yest|daycode>"
		sys.exit(1)


	exclist = SynchUtil.getCheckExcList(sys.argv[1])	
	loglist = SynchUtil.getCheckLogList(sys.argv[2])
	daylist = SynchUtil.getCheckDayList(sys.argv[3])
	
	# need to make sure we're running in a folder where the .mani files
	# can be written
	os.chdir('/var/log/cronlogs/hdfs/manifiles/')

	for onex in exclist:
		for logtype in loglist:
			for daycode in daylist:
				
				if not SynchUtil.nfsFilesExist(onex, logtype, daycode):
					print "No NFS files for %s %s %s" % (onex, logtype, daycode)
					continue				
				
				#os.system("echo \"calling simplesynch for %s %s %s\" | wall" % (onex, logtype, daycode))
				doSimpleSynch(onex, logtype, daycode)
				#logmessg = "Finished SimpleSynch for %s %s %s" % (onex, logtype, daycode)
				#logmail.addLogLine(logmessg)
	
	#logmail.send2admin()

