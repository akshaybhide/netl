#!/usr/bin/python

###################################################
# Grab an Exelate file from anacluster07, upload it to Hadoop
###################################################

import os, sys

sys.path.append("/local/src/cronjobs/")

import SimpleMail, SynchUtil

PRIV_KEY_PATH = "/home/burfoot/.ssh/priv_key_342"
REMOTE_EXELATE_DIR = "/home/exelate-data/incoming"

TEMP_EX_PATH = "/tmp/exelate/TEMP_EX_FILE.txt"
SFTP_HOST = "anacluster07.adnetik.iponweb.net"

HDFS_EX_PATH = "/thirdparty/exelate/dump"
	
def getSftpPath(daycode):
	
	stripday = daycode.replace("-", "")
	return "%s/exelate_%s.tsv" % (REMOTE_EXELATE_DIR, stripday)

def getHdfsPath(daycode):
	
	return "%s/dump_%s.tsv" % (HDFS_EX_PATH, daycode)
	

def grabExelateFile(daycode):
	
	sftpcall = "sftp -i %s burfoot@%s:%s %s" % (PRIV_KEY_PATH, SFTP_HOST, getSftpPath(daycode), TEMP_EX_PATH)
	os.system(sftpcall);
	
	print "Syscall is %s" % (sftpcall)
	

def upload2hdfs(daycode):
	
	print "Uploading temp file to %s..." % (getHdfsPath(daycode))
	hadcall = "hadoop fs -put %s %s" % (TEMP_EX_PATH, getHdfsPath(daycode))
	os.system(hadcall)
	print "... done"

if __name__ == "__main__":
	
	daylist = []
	
	if len(sys.argv) < 2:
		print "Usage UploadExelate.py <daycode|daylist.txt>"
		sys.exit(1)
		
	singarg = sys.argv[1]
	if singarg.endswith(".txt"):
		gimp = [daylist.append(oneday.strip()) for oneday in open(singarg)]
	elif singarg == "yest":
		daylist.append(SynchUtil.get_yesterday())
	else:
		daylist.append(singarg)

	
	
	for daycode in daylist:
	
		grabExelateFile(daycode)
		
		upload2hdfs(daycode)		




	

	
#pc_set = set()
#for x in targlist:
#	pc_set.add(x)
#	pc_set.add(pix_comp_map[x])
#
#for onepix in pc_set:
#	
#	precompPix(onepix)
#	logmail.addLogLine("Finished precomputation for pixel %s" % (onepix))
#	
#for onepix in targlist:
#	genUserReport(onepix, pix_comp_map[onepix])
#	logmail.addLogLine("Finished user report for pixel comparison %s - %s" % (onepix, pix_comp_map[onepix]))
#	
#	#autoLearn(onepix, pix_comp_map[onepix])
#	#logmail.addLogLine("Finished AdaBoost learning for pixel comparison %s - %s" % (onepix, pix_comp_map[onepix]))
#		
#		
#logmail.send2admin()

