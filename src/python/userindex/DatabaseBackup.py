#!/usr/bin/python

###################################################
# Backup Userindex DB
###################################################

import os, sys
from time import gmtime, strftime

sys.path.append("/home/burfoot/src/python/codeutil")
sys.path.append("/home/burfoot/src/python/shared")

import Util

import hadJavaCall, ArgMap
	
if __name__ == "__main__":

	print "hello, data backup script"
	
	backupdir = "/local/fellowship/userindex/backup/%s" % (Util.get_today())
	
	if not os.path.exists(backupdir):
		os.mkdir(backupdir)
	
	
	backuppath = "%s/db_dump.sql.gz" % (backupdir)
		
	dumpcall = "mysqldump -u burfoot -h thorin.adnetik.com -pdata_101? userindex "
	dumpcall += " | gzip " 
	dumpcall += " > %s" % (backuppath)
		
	print "Starting dump, time is %s" % (strftime("%Y-%m-%d %H:%M:%S", gmtime()))
	print "Dumping to file %s" % (backuppath)
	#print "Dump call is %s" % (dumpcall)
	os.system(dumpcall)
	print "Finished dump, time is %s" % (strftime("%Y-%m-%d %H:%M:%S", gmtime()))

