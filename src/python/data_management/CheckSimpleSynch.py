#!/usr/bin/python

###################################################
# 1) generate a manifest file
# 2) put on Hadoop 
# 3) hadoop distcp using a system call
# 4) remove mani file
###################################################

import os, sys
import SynchUtil

def getNfsSizeMap(adex, logtype, daycode):

	sizeMap = {}
	nfsdirpath = SynchUtil.getNfsDirPath(adex, logtype, daycode)
		
	for filename in os.listdir(nfsdirpath):
		
		if filename.find('.gz') == -1:
			continue		
		
		filesize = os.path.getsize(nfsdirpath + "/" + filename)
		sizeMap[filename] = filesize
				
	return sizeMap


def getS3sizeMap(adex, logtype, daycode):
	
	sizemap = {}
		
	s3buck = SynchUtil.s3BucketGrab()
	s3keyl = SynchUtil.s3KeyList(s3buck, adex, logtype, daycode)
	
	for key in s3keyl:
		simpname = key.name.split('/')[-1]
		sizemap[simpname] = key.size

	return sizemap
	
def getHDsizeMap(adex, logtype, daycode):
	
	sizeMap = {}
	hadlscall  = "hadoop fs -ls /data/%s/%s/%s/" % ( logtype, daycode, adex )
	
	hadooplines = SynchUtil.sysCallResult(hadlscall)
	
	for line in hadooplines:
		toks = line.split()
		
		if line.find('.gz') == -1:
			continue
		
		filename = toks[7].split('/')[-1]
		filesize = int(toks[4])
		sizeMap[filename] = filesize
		
	return sizeMap


def checkSimpleSynch(adex, logtype, daycode):
	
	aMap = getNfsSizeMap(adex, logtype, daycode)
	bMap = getHDsizeMap(adex, logtype, daycode)
	
	misslist = [fname for fname in aMap if not fname in bMap]
	
	sizelist = []
	
	for fname in bMap:
		
		if not fname in aMap:
			print "Error: file name %s not found in NFS map" % (fname)
			continue
			
		if not aMap[fname] == bMap[fname]:
			sizelist.append(fname)
	
		
	
	#sizelist = [fname for fname in bMap if not aMap[fname] == bMap[fname]]
	
	if len(misslist) + len(sizelist) == 0:
		print "No mismatches for %s %s %s" % (adex, logtype, daycode)
		
	for missfile in misslist:
		print "File is MISSING: %s" % (missfile)
	
	for sizefile in sizelist:
		print "File %s \n\t has incorrect size of %d, should be %d" % (sizefile, bMap[sizefile], aMap[sizefile])
	
	
if __name__ == "__main__":

	if not len(sys.argv) == 4:
		print "Usage: SimpleSynch <all|adex> <big|mini|comp|logtype> <yest|daycode>"
		sys.exit(1)

	exclist = SynchUtil.getCheckExcList(sys.argv[1])	
	loglist = SynchUtil.getCheckLogList(sys.argv[2])
	daylist = SynchUtil.getCheckDayList(sys.argv[3])
	
	# need to make sure we're running in a folder where the .mani files
	# can be written
	os.chdir('/mnt/src/cronjobs/')
	
	for onex in exclist:
		
		# DBH has no Big Data logs
		if onex == 'dbh':
			continue
		
		for logtype in loglist:
			for daycode in daylist:
				#getNfsSizeMap(onex, logtype, daycode)
				checkSimpleSynch(onex, logtype, daycode)
	

