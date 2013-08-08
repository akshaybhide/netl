#!/usr/bin/python

import re, os, sys, fileinput, LocalConf




if __name__ == "__main__":


	extralist = []	
	extralist.append("com/adnetik")
	extralist.append("com/digilant")

	# Base dir to number of delete files
	delmap = {}

	for onedir in extralist:
		
		fulldir = "%s/%s" % (LocalConf.getJavaClassDir(False), onedir)
		
		for (basedir, dirlist, flist) in os.walk(fulldir):
			
			for onefile in flist:
				if onefile.endswith(".class"):
					
					fullpath = "%s/%s" % (basedir, onefile)
					os.remove(fullpath)
					
					delmap.setdefault(basedir, 0)
					delmap[basedir] += 1
					


	for deldir in delmap:
		print "Deleted %d class files in %s" % (delmap[deldir], deldir)
