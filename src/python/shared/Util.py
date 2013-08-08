#!/usr/bin/python

###################################################
# Utilities to help with the data synch between Hadoop
# and the NFS log files
###################################################

import os, sys, datetime, random, fileinput

# TODO: this information is replicated in ConcatLzoSynch, move to central location
JAR_PATH = '/local/bin/jars/adnetik.jar'
LZO_JAR_PATH = '/mnt/jars/hadoop-0.20/lib/hadoop-lzo-0.4.9.jar'


NFS_BASE = '/mnt/adnetik/adnetik-uservervillage'

LOCAL_TEMP_COPY = '/mnt/data/temp_nfs_copy'

#/mnt/adnetik/adnetik-uservervillage/<exch_code>/userver_log/<logtype>/<daycode>/filename.log.gz

FILE_URL_PREF = "file://"

BIG_PRINT_BAR = "------------------------------------------------"

def promptOkay(messg):
	
	print "%s [yes/NO]: " % (messg)
	line = raw_input().strip()
	return "yes" == line


def sysCallResult(syscall):

	if syscall.find(">") > -1:
		print "Error: cannot include redirect"
		sys.exit(1)

	# Hack method
	tmpfile = gimmeTemp()
	syscallred = "%s > %s" % (syscall, tmpfile)
	os.system(syscallred)
	
	result = [line for line in open(tmpfile)]
	os.remove(tmpfile)
	
	return result

def gimmeTemp():
	tempid = int(random.random() * 1000000)
	return "/tmp/py_temp_%d" % (tempid)
	
	
def get_prevday(numback):
	
	now = datetime.datetime.now()
	d1 = datetime.timedelta(days=numback)
	prevday = now-d1
	prevday_str = prevday.strftime("%Y-%m-%d")
	return prevday_str
	
def get_yesterday():
	"""
	Returns yesterday's timestamp as a string in format YYYY-MM-DD
	"""
	return get_prevday(1)
	
def get_today():
	now = datetime.datetime.now()
	return now.strftime("%Y-%m-%d")
	
