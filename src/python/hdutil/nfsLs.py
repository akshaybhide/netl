#!/usr/bin/python

import re, os, sys
			
if __name__ == "__main__":

	"""
	Shortcut for ls-ing NFS mount files
	"""
	
	if len(sys.argv) < 3:
		print "Usage: nfsLs.py <adex> <logtype> <daycode>"
		sys.exit(1)

	(adex, log_type, daycode) = sys.argv[1:4]
		
	lsargs = "-al" if not len(sys.argv) == 5 else sys.argv[4]
		
	# /mnt/adnetik/adnetik-uservervillage/adnexus/userver_log/no_bid/2011-10-24/2011-10-24-23-59-59.EDT.no_bid_v12.adnexus-rtb-ireland_5e37d.log.gz
	
	nfsdir = "/mnt/adnetik/adnetik-uservervillage/%s/userver_log/%s/%s/" % (adex, log_type, daycode)
	lscall = "ls %s %s" % (lsargs, nfsdir)
	
	os.system(lscall)

