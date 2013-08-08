#!/usr/bin/python

import os,random,time,datetime,sys
		
if __name__ == "__main__":

	listmap = {}

	for oneline in sys.stdin:	
		
		(listcode, score) = oneline.strip().split("\t")
		listmap.setdefault(listcode, [])
		
		listmap[listcode].append(float(score))
		
		
	for onelist in listmap :
		
		fhandle = open('eval_%s.csv' % (onelist), 'w')
		
		revlist = [x for x in sorted(listmap[onelist])]
		revlist.reverse()
		
		gimp = [fhandle.write(str(x)+"\n") for x in revlist]
			
		fhandle.close()	

		print "Found %d scores for listcode %s" % (len(listmap[onelist]), onelist)
		
		# Test of GIT checkin technology
		
