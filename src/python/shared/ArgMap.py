#!/usr/bin/python

import re, os, sys, fileinput

def getClArgMap(argv):
	amap = ArgMap()
	amap.putClArgs(argv)
	return amap

class ArgMap:
	
	_dataMap = {}

	
	def putClArgs(self, args):
		
		for onearg in args:
			if "=" in onearg:
				(key, val) = onearg.split("=")
				self._dataMap[key] = val
	
	def getBoolean(self, key, booldef):

		if not self.containsKey(key):
			return booldef

		bstr = self._dataMap[key].lower()		
		assert "false" in bstr or "true" in bstr, "Invalid boolean string %s" % (bstr)
		return False if "false" in bstr else True
		
	def getInt(self, key, intdef):
		return int(self._dataMap[key]) if self.containsKey(key) else intdef

	def getString(self, key, strdef):
		return str(self._dataMap[key]) if self.containsKey(key) else strdef

	def containsKey(self, key):
		return key in self._dataMap
		
		
		
	def setString(self, key, val):
		self._dataMap[key] = val
		
	def showInfo(self):
		for onekey in self._dataMap:
			print "Key is %s --> %s" % (onekey, self._dataMap[onekey])

if __name__ == "__main__":


	amap = ArgMap()

	amap.putClArgs(sys.argv)
	
	amap.showInfo()
	
	print "Startfrom is %d" % (amap.getInt("startpart", 0))
	print "Mybool is %r" % (amap.getBoolean("mybool", False))
