#!/usr/bin/python

import sys, boto, os
import PyS3Synch

from boto.s3.key import Key
	
def getPixelFiles(date):
	"""
	Returns a list of pixel files for a given date.
	Standard date code (e.g. 2011-08-03)
	"""
	s3 = boto.connect_s3()
	bucket = s3.get_bucket('adnetik-uservervillage')
	
	# Bucket in hand, jack and jill can go up the hill to fetch a pail of data
	data_path = '/'.join(['prod','userver_log','pixel',date])
	filelist = [fname.name for fname in bucket.list(data_path)]
	
	return filelist

usage = 'Usage:\n\t SynchPixel.py <datefile> localdir \n\t EX: SynchPixel.py 2011-07-19 tempdir \n'

if __name__ == "__main__":

	if len(sys.argv) < 2:
		print usage
		sys.exit(1)

	#print "file list is %s" % (str(sys.argv))

	# date info is either a file name or a single code
	dateinfo, tempdir = sys.argv[1:]

	filelist = []
	
	if dateinfo in os.listdir('.'):
		for onedate in open(dateinfo):
			#print "one date is " + str(onedate)
			filelist.extend(getPixelFiles(onedate.strip()))
			
	else:
		filelist.append(dateinfo)
		
	for filename in filelist:    	    
		if(filename.split('.')[-1] == 'gz'):
			PyS3Synch.downloadToTemp(filename, tempdir)

