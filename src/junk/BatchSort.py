#!/usr/bin/python

###################################################
# 1) Grab a No_Bid file from HDFS
# 2) Pull out the WTP id, and maybe scrub it
# 3) Put it into a batch based on a hashcode of the filename
# 4) When done with all the files, sort each batch.
###################################################

import os, time, sys, datetime, random, fileinput
import LogFileDataSize, SynchUtil

GIMP_FILE = "/mnt/data/tmp/burfoot/userindex/gimp.txt"

def zpad(x, tlen):
	
	s = str(x)
	
	while len(s) < tlen:
		s = "0" + s
		
	return s
	
def getTempFilePath(proc_id):
	return "/mnt/data/tmp/burfoot/userindex/batch_temp_%d.gz" % (proc_id)
	
def getGimpFile(proc_id):
	return "/mnt/data/tmp/burfoot/userindex/gimp_file_%d.txt" % (proc_id)
	
def getBatchDir(daycode):
	return "/mnt/data/tmp/burfoot/userindex/%s/" % (daycode)

def batchFilePath(daycode, hashcode):
	return "/mnt/data/tmp/burfoot/userindex/%s/batch_%s.txt" % (daycode, zpad(hashcode,3))

def getManiFilePath(daycode):
	return getBatchDir(daycode) + "bigmani.txt"
	
def getManiData(daycode):
	
	manimap = {}
	
	for line in open(getManiFilePath(daycode)):
		(onefile, fsize) = line.strip().split("\t")
		manimap[onefile] = fsize
		
	return manimap
	
def setupManiFile(daycode):
	
	batchdir = getBatchDir(daycode)
	
	if not os.path.exists(batchdir):
		os.system("mkdir %s" % batchdir)
	
	manimap = {}
	
	for onex in SynchUtil.getExchanges():
		for logtype in SynchUtil.getBigLogList():
			sizemap = LogFileDataSize.getHdfsPathList(onex, logtype, daycode)
			
			for onefile in sizemap:
				manimap[onefile] = sizemap[onefile]
			
			sizegb = sum(sizemap.values()) / 1000000000
			
			print "Found %d files, %d gb for %s %s %s" % (len(sizemap), sizegb, onex, logtype, daycode)
		
	
	
	fhandle = open(getManiFilePath(daycode), 'w')
	for onefile in manimap:
		fhandle.write(onefile + "\t" + str(manimap[onefile]) + "\n")
	fhandle.close()
	
	print "Finished building manifest, %d total files and %d gb size" % (len(manimap), sum(manimap.values())/1000000000)
	
	
def sortBatchFile(daycode, batchcode, proc_id):
	
	gimpfile = getGimpFile(proc_id)
	batchfile = batchFilePath(daycode, batchcode)
	print "Sorting batchfile %s ... " % (batchfile),

	# TODO: use a special temp dir for sorting
	sortcall = "sort %s > %s" % (batchfile, gimpfile)
	os.system(sortcall)
	
	print " ... done"
	mvcall = "mv %s %s" % (gimpfile, batchfile)
	os.system(mvcall)
	
	mvcall = "mv %s %s" % (batchfile, batchfile + ".sorted")
	os.system(mvcall)	
	


def grabHdfsFile(filename, proc_id):
	
	temp_file = getTempFilePath(proc_id)
	
	# TODO check for existence, then delete if necessary
	rmcall = "rm %s" % (temp_file)
	os.system(rmcall)
	
	hadcall = "hadoop fs -get %s %s" % (filename, temp_file)
	os.system(hadcall)

# Grab the file directly from Hadoop, pipe it through gunzip and ScrubPull, output to the batch file
def grabToBatch(filename, daycode, hash_code):
	
	batchfile = batchFilePath(daycode, hash_code)
	javacall = "java -cp /mnt/jars/adnetik.jar com.adnetik.userindex.ScrubPull"
	
	hadcall = "hadoop fs -cat %s | gunzip | %s > %s" % (filename, javacall, batchfile)
	#print hadcall
	os.system(hadcall)

if __name__ == "__main__":

	if not len(sys.argv) == 3: 
		print "Usage BatchSort.py daycode a/b/c"
		sys.exit(1)
		
	daycode = sys.argv[1]
	if "yest" == daycode:
		daycode = SynchUtil.get_yesterday()
	
	(proc_id, num_proc, num_batch) = [int(x) for x in sys.argv[2].split("/")]
	if int(proc_id) == 0:
		setupManiFile(daycode)
	
	# TODO: don't use temp file, instead hadoop cat into another program 
	# that does the scrubbing, and direct output to the batch file.
	print "proc=%d, num=%d, nbatch=%d" % (proc_id, num_proc, num_batch)

	manimap = getManiData(daycode)
	
	# TODO: prefilter files so that we can print "finished with file 52/652..."
	targlist = [onefile for onefile in manimap if ((hash(onefile) % num_batch) % num_proc) == proc_id]
	
	for fcount in range(len(targlist)):

		onefile = targlist[fcount]
		hash_code = hash(onefile) % num_batch
		proc_code = hash_code % num_proc
		
		# Skip files that don't match this proc code
		if not proc_code == proc_id:
			print "error, found bad code"
						
		grabToBatch(onefile, daycode, hash_code)
										
		#grabHdfsFile(onefile, proc_id)		
		#numwrote = outputToBatch(daycode, hash_code, proc_id)
		nowtime = datetime.datetime.now().isoformat()[:-7]
		
		print "%s: finished file %d/%d size %d mb" % (nowtime, fcount, len(targlist), manimap[onefile]/1000000)		
		
		
#	for hash_code in range(num_batch):
#		proc_code = hash_code % num_proc
#		
#		if proc_code == proc_id:
#			sortBatchFile(daycode, hash_code, proc_id)
#			
			
			
#def outputToBatch(daycode, hashcode, proc_id):
#	
#	lcount = 0
#	batch_file = batchFilePath(daycode, hashcode)
#	temp_file = getTempFilePath(proc_id)
#	
#	fhandle = open(batch_file, 'a')
#	
#	for line in fileinput.input(temp_file, openhook=fileinput.hook_compressed):
#		
#		toks = line.strip().split("\t")
#		wtp = toks[99]
#		
#		if len(wtp) < 30:
#			continue
#						
#		fhandle.write(wtp)
#		fhandle.write("\t")
#		fhandle.write(line)
#		fhandle.write("\n")
#		lcount += 1
#		
#		# TODO: Take out later
#		#if lcount > 10000:
#		#	break
#		
#	fileinput.close()
#	fhandle.close()
#	
#	return lcount
