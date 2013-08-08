#!/usr/bin/python

import re, os, sys, fileinput
			
if __name__ == "__main__":

	cwd = os.getcwd()
	hadcall = "hadoop fs -ls %s" % (cwd)
	os.system(hadcall)
