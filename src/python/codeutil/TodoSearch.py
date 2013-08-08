#!/usr/bin/python

import os,random,time,datetime,sys

sys.path.append("/home/burfoot/src/python/util")

import CompJava,LocalConf

def check4ToDo(checkdir, doprint=False):
	
	tcount = 0
	btcount = 0
	isjava = "java" in checkdir
	
	for onefile in os.listdir(checkdir):
		if isjava and onefile.endswith("java"):
			
			flines = [oneline for oneline in open("%s/%s" % (checkdir, onefile))]
			for i in range(len(flines)):
				
				if "BIGTODO" in flines[i]:
					btcount += 1
					print "*****WARNING******, BIG TODO found in file %s" % (onefile)
				
				if "TODO" in flines[i]:
					tcount += 1
					if doprint:
						print "TODO found on line %d of %s: %s" % (i, onefile, flines[i]),


	return tcount


if __name__ == "__main__":
	
	packlist = CompJava.getPackageList()
		
	for onepack in packlist:
		srcdir = CompJava.getSrcDir4Pack(onepack)
		td4pack = check4ToDo(srcdir)
		print "Found %d TODOs in package %s" % (td4pack, srcdir)
