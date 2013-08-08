#!/usr/bin/python

import re, os, sys


if __name__ == "__main__":

	"""
	Checks ability to call jobs from crontab
	
	"""
	
	hadcall = "hadoop jar /local/bin/jars/adnetik.jar com.adnetik.hadtest.SimpleTestJob"
	#hadcall = "hadoop fs -ls /userindex/"
	
	os.system(" echo \"hello from cron\" | wall")
	
	os.system(hadcall)
	
#danb
