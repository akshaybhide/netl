#!/usr/bin/python

import re, os, sys, fileinput, random

# Hack
sys.path.append("/mnt/src/cronjobs")
import SynchUtil

HDFS_ADA_DIR = "/userindex/adaclass"

def pixprefFromDir(pixdir):
	
	dirtoks = pixdir.split("_")
	return "_".join(dirtoks[1:])

def cleanHdfsDirs():
	
	cleancall = "hadoop fs -rmr %s/*" % (HDFS_ADA_DIR)
	print "Cleaning adaclass dir with command %s" % (cleancall)
	
	if SynchUtil.promptOkay("Okay to delete? "):
		os.system(cleancall)
	else:
		print "Okay, quitting"
		sys.exit(1)
		

def createHdfsDirs(pixset):
	
	extantdirs = SynchUtil.sysCallResult("hadoop fs -ls %s" % HDFS_ADA_DIR)
	
	for onepix in pixset:
		
		pixpref = pixprefFromDir(onepix)
		
		if any([pixpref in exline for exline in extantdirs]):
			print "Found directory %s" % (pixpref)
		else:
			hadmkdir = "hadoop fs -mkdir %s/%s" % (HDFS_ADA_DIR, pixpref)
			print "Mkdir call is %s" % (hadmkdir)
			os.system(hadmkdir)


def uploadAdaData(locdir, listset):
	
	for subpixdir in listset:
		
		pixpref = pixprefFromDir(subpixdir)
		hdfsdir = "/userindex/adaclass/%s/" % (pixpref)
		
		#rmcall = "hadoop fs -rmr %s
		
		hadcall = "hadoop fs -put %s%s/*.ser %s" % (locdir, subpixdir, hdfsdir)
		
		print "Upload call is %s" % (hadcall)
		os.system(hadcall)
	

if __name__ == "__main__":

	daycode = SynchUtil.get_today() if len(sys.argv) < 2 else sys.argv[1]

	print "\nCalling for daycode %s" % (daycode)
	
	locadadir = "/mnt/data/userindex/%s/" % (daycode)
	
	listset = set()
	
	for onedir in os.listdir(locadadir):
		
		if "adaclass_" in onedir:
			
			serfiles = [onefile for onefile in os.listdir(locadadir + "/" + onedir) if ".ser" in onefile]
			
			if len(serfiles) > 0:
				print "Found directory %s with %d serfiles" % (onedir, len(serfiles))
				listset.add(onedir)
			else:
				print "Found directory %s, but no serfiles found, skipping" % (onedir)
			
	
	cleanHdfsDirs()

	createHdfsDirs(listset)
	
	uploadAdaData(locadadir, listset)
