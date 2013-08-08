#!/usr/bin/python

import sys, os, datetime, SynchUtil

def getHdfsPathList(excname, logtype, daycode):
	
	hadlscall = "hadoop fs -ls /data/%s/%s/%s/ > hdfsls.txt" % ( logtype, daycode, excname )
	os.system(hadlscall)
	sizemap = {}
	
	for line in open('hdfsls.txt'):
		
		if line.find('.gz') == -1:
			continue 
			
		toks = line.strip().split(" ")
		fname = toks[-1]
		fsize = toks[-4]
		sizemap[fname] = int(fsize)
		
	return sizemap	
		
if __name__ == "__main__":

	if not len(sys.argv) == 4:
		print "Usage: LogFileDataSize <all|adex> <big|mini|comp|logtype> <yest|daycode>"
		sys.exit(1)

	exclist = SynchUtil.getCheckExcList(sys.argv[1])	
	loglist = SynchUtil.getCheckLogList(sys.argv[2])
	daylist = SynchUtil.getCheckDayList(sys.argv[3])

	totcount = 0
	totsize = 0

	for onex in exclist:
		for logtype in loglist:
			for daycode in daylist:
				
				sizemap = getHdfsPathList(onex, logtype, daycode)
				
				batchcount = len(sizemap)
				batchsize = sum([sizemap[fname] for fname in sizemap]) / 1000000000
				
				print "For data batch %s/%s/%s" % (onex, logtype, daycode)				
				print "\t%d files\n\ttotal size %d gb" % (batchcount, batchsize)
				
				totcount += batchcount
				totsize += batchsize
				
	print "Total of %d files, total size %d gb" % (totcount, totsize)
	

