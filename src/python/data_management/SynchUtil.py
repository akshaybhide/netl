#!/usr/bin/python

###################################################
# Utilities to help with the data synch between Hadoop
# and the NFS log files
###################################################

import os, sys, datetime, random, fileinput

# TODO: this information is replicated in ConcatLzoSynch, move to central location
JAR_PATH = '/local/bin/jars/adnetik.jar'
LZO_JAR_PATH = '/mnt/jars/hadoop-0.20/lib/hadoop-lzo-0.4.9.jar'

LOG_SYNC_CLASS = 'com.adnetik.data_management.LogSync'
PIX_LOG_SYNC_CLASS = 'com.adnetik.data_management.PixelLogSync'


S3N_PREF = 's3n://adnetik-uservervillage/'

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

def getManiPath(exchange, logtype, daycode):
	
	return "%s___%s___%s.mani" % ( exchange, logtype, daycode)

def getHdfsPath(exchange, logtype, daycode):
	
	return "/data/%s/%s_%s.lzo" % ( logtype, exchange, daycode)


def getNfsDirPath(exchange, logtype, daycode):
	
	nfsdirpath = "%s/%s/userver_log/%s/%s/" % (NFS_BASE, exchange, logtype, daycode)
	return nfsdirpath
	
def nfsFilesExist(exchange, logtype, daycode):
	
	nfsdir = getNfsDirPath(exchange, logtype, daycode)
	
	if not os.path.exists(nfsdir):
		return False
		
	# This actually shouldn't happen, but there is currently a bug in IOW's
	# file writing process
	if not os.access(nfsdir, os.R_OK):
		return False
		
	return len(os.listdir(nfsdir)) > 0

def writeManiFile(exchange, logtype, daycode, usepref=FILE_URL_PREF, writesize=False):
	
	manifilepath = getManiPath( exchange, logtype, daycode )
	mhandle = open(manifilepath, 'w')
	keycount = 0

	#/mnt/adnetik/adnetik-uservervillage/<exch_code>/userver_log/<logtype>/<daycode>/filename.log.gz
	dirpath = "%s/%s/userver_log/%s/%s/" % (NFS_BASE, exchange, logtype, daycode)	
	
	for onefile in os.listdir(dirpath):
		keycount += 1
		mhandle.write(usepref + dirpath + onefile)
		if writesize:
			fsize = os.path.getsize(dirpath + "/" + onefile)
			mhandle.write("\t" + str(fsize))
		
		mhandle.write("\n")
		
	mhandle.close()	
	print "Wrote %d keys to manifest file %s" % (keycount, manifilepath)
	return manifilepath
	
def getMiniLogList():

	return ['imp', 'click', 'conversion']

def getBigLogList():
	
	# Added 'bid_pre_filtered' on 2012-08-28
	# Changed no_bid --> no_bid_all
	return [ 'no_bid_all', 'bid_all', 'bid_pre_filtered' ]
	
def getCompleteLogList():

	a = getMiniLogList()
	a.extend(getBigLogList())
	return a	
	
def getCheckLogList(logarg):
	
	if logarg == 'comp':
		return getCompleteLogList()
		
	if logarg == 'mini':
		return getMiniLogList()
		
	if logarg == 'big':
		return getBigLogList()
		
	if logarg in getCompleteLogList():
		return [logarg]
		
	print "Error: bad log list argument %s" % (logarg)
	sys.exit(1)

def getExchanges():
	"""
	Returns a list of all exchanges
	"""
	return  ['adbrite','admeld','adnexus','casale','contextweb', 'facebook',
                 'improvedigital','nexage','openx','rtb','rubicon','yahoo', 'dbh', 'admeta']

def getCheckExcList(excarg):

	if excarg == 'all':
		return getExchanges()
	
	if excarg in getExchanges():
		return [excarg]
		
	print "Error: bad exchange argument: %s" % (excarg)
	sys.exit(1)
	
def getCheckDayList(dayarg):

	# Interpret the argument as a file containing a list of day codes
	if dayarg.find('.txt') > -1:
		
		daylist = []
		
		# recursively call this function on each line, to ensure
		# that the formatting of the dates are correct
		for line in open(dayarg):
			daylist.extend(getCheckDayList(line.strip()))
		
		return daylist

	if dayarg == 'yest':
		return [get_yesterday()]
		
	try:
		ymdlist = dayarg.split('-')
		ymdints = [int(ymd) for ymd in ymdlist]
	except Exception:
		print "Bad day format: %s" % (dayarg)
		sys.exit(1)
	
	return [dayarg]
	
def s3BucketGrab():
	"""
	Gets the adnetik-uservervilage bucket from S3
	"""
	s3 = boto.connect_s3('AKIAJQSQ6DAW3LXRD2CA','VMbpYn+36mGuDqMd9mOa/NeoF4tN0+AAuJa+T5TK')
	bucket = s3.get_bucket('adnetik-uservervillage')

   	return bucket

def s3KeyList(bucket,exchange,log_type,day):
	"""
	Returns a list of files for a given exchange, log type 
	and date in the s3 bucket -adnetik-userver-village
	Exchanges need to be referenced correctly:
    	ex. rtb, rubicon, admeld, etc
    	Date should be in form YYYY-MM-DD ex. 2011-07-19
    	"""
    	# Bucket in hand, jack and jill can go up the hill to fetch a pail of data
    	if log_type=='pixel':
    		data_path = 'prod/userver_log/pixel/'+day
    	else:
    		data_path = '/'.join([exchange,'userver_log',log_type,day])
    		
	keylist = [s3key for s3key in bucket.list(data_path) if s3key.name.split('.')[-1]=='gz']
    
	return keylist

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
