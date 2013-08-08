#!/usr/bin/python

###################################################
# 1) generate a manifest file
# 2) put on Hadoop 
# 3) hadoop distcp using a system call
# 4) remove mani file
###################################################

import os, sys
import SynchUtil

def getBkFileList(daycode):
	
	bknfsdir = getBluekaiNfsDir(daycode)
	return ["%s/%s" % (bknfsdir, onefile) for onefile in os.listdir(bknfsdir)]

def getBluekaiNfsDir(daycode):
	
	return "/mnt/adnetik/adnetik-uservervillage/prod/userver_log/bk_data/%s" % (daycode)

def getBluekaiHdfsDir(daycode):
	return "/data/bk_data/%s" % (daycode)

def writeManiFile(daycode):
	
	tmpmanipath = "/tmp/mani/bk_mani_%s.txt" % (daycode)
	fhandle = open(tmpmanipath, 'w')
	fhandle.writelines(["file://%s\n" % (onepath) for onepath in getBkFileList(daycode)])
	fhandle.close()
	return tmpmanipath

def doBkSimpleSynch(daycode):
		
	# Generate a manifest file, using appropriate prefix
	manipath = writeManiFile(daycode)
	
	# Put the manifest file on HDFS,
	# Use same tmp mani path
	putCall = "hadoop fs -put %s %s" % (manipath, manipath)
	print "PutCall: %s" % (putCall)
	os.system(putCall)
	
	# run distcp
	hdfsdir = getBluekaiHdfsDir(daycode)
	dist_cp_call = "hadoop distcp -f %s %s" % (manipath, hdfsdir)
	print "DistCpCall: %s" % (dist_cp_call)
	os.system(dist_cp_call)

	## delete mani file
	hdfsRmCall = "hadoop fs -rm %s" % (manipath)
	print hdfsRmCall
	os.system(hdfsRmCall)
	
	## Delete local file
	locRmCall = "rm %s" % (manipath)
	print "Local rm call is %s" % (manipath)
	os.system(locRmCall)
	
	
if __name__ == "__main__":

	#if not len(sys.argv) == 4:
	#	print "Usage: SimpleSynch <all|adex> <big|mini|comp|logtype> <yest|daycode>"
	#	sys.exit(1)

	# This is kind of the hacky way to do things
	#sys.path.append("/local/src/python/util")
	#import SimpleMail
	#logmail = SimpleMail.SimpleMail("SimpleSynch Report")
        #
        #
	#exclist = SynchUtil.getCheckExcList(sys.argv[1])	
	#loglist = SynchUtil.getCheckLogList(sys.argv[2])
	#daylist = SynchUtil.getCheckDayList(sys.argv[3])
	
	# need to make sure we're running in a folder where the .mani files
	# can be written
	#os.chdir('/var/log/cronlogs/hdfs/manifiles/')

	daycode = "2012-09-27"
	doBkSimpleSynch(daycode)
	#tmpmani = writeManiFile(daycode)
	
	#print "Wrote mani data to %s" % (tmpmani)

	#bkfiles = getBkFileList(daycode)
	
	#for onebk in bkfiles:
	#	print "One bk file is %s" % (onebk)
	

	#for onex in exclist:
	#	for logtype in loglist:
	#		for daycode in daylist:
	#			
	#			if not SynchUtil.nfsFilesExist(onex, logtype, daycode):
	#				print "No NFS files for %s %s %s" % (onex, logtype, daycode)
	#				continue				
	#			
	#			#os.system("echo \"calling simplesynch for %s %s %s\" | wall" % (onex, logtype, daycode))
	#			doSimpleSynch(onex, logtype, daycode)
	#			#logmessg = "Finished SimpleSynch for %s %s %s" % (onex, logtype, daycode)
	#			#logmail.addLogLine(logmessg)
	#
	#logmail.send2admin()

