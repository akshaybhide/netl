#!/usr/bin/python

import re, os, sys, fileinput
	
	
def checkUserData(oneid, mapa, mapb):
	assert mapa.keys() == mapb.keys(), "For ID %s, found keyset \n%s in A and \n%s in B" % (oneid, mapa.keys(), mapb.keys())	
	
	for oneday in mapa:
		
		assert mapa[oneday] == mapb[oneday], "For ID %s daycode %s, found \n%s vs \n%s" % (oneid, oneday, mapa[oneday], mapb[oneday])
		
	
class SameCheck:
	
	
	def __init__(self, fpath):
		
		self._infoMap = {}
		
		for oneline in open(fpath):
			self.addLine(oneline)
			

	def addLine(self, oneline):
		
		(wtp, daycode, seglist, junk) = oneline.split("\t")
		
		self._infoMap.setdefault(wtp, {})
		
		assert not daycode in self._infoMap[wtp], "For ID %s: ound repeated daycode %s, dayset is %s" % (daycode, self._infoMap[wtp])
		
		intseglist = [int(oneseg) for oneseg in seglist.split(",")]
		
		self._infoMap[wtp][daycode] = intseglist
		
		
	def checkAgainst(self, other):
		
		assert self._infoMap.keys() == other._infoMap.keys(), "SELF keyset is %s, OTHER is %s" % (self._infoMap.keys(), other._infoMap.keys())
		
		for oneid in self._infoMap:
			checkUserData(oneid, self._infoMap[oneid], other._infoMap[oneid])
		

	
def shortCopyWrite(srcpath, dstpath, nlines):
	
	lcount = 0
	
	fhandle = open(dstpath, 'w')
	
	for oneline in open(srcpath):
		fhandle.write(oneline)
		
		lcount += 1
		if lcount > nlines:
			break

	fhandle.close()
	
if __name__ == "__main__":
	
	print "Going to check-same"
	
	nlines = 10000
	
	shortCopyWrite('GIMP_04-20_backup.txt', 'methA.txt', nlines)
	shortCopyWrite('__GIMP_MASTER_2013-04-20.txt', 'methB.txt', nlines)
	
	checkA = SameCheck('methA.txt')
	checkB = SameCheck('methB.txt')
	
	checkA.checkAgainst(checkB)
	
	print "Successful test of A vs B"
	
	
	
