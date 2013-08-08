#!/usr/bin/python

import os,random,time,datetime,sys

sys.path.append("/local/src/python/util")

import CompJava, LocalConf, GraphImp
	
def importSet4File(jfilepath):
	
	#assert jfilepath.endswith(".java"), "Argument file %s is not a java src file" % (jfilepath)

	impset = set()

	if jfilepath.endswith(".java"):
		
		#print "Checking file %s" % (jfilepath)
		
		for oneline in open(jfilepath):
			for onefull in getFullPackList():
				if oneline.find('import') > -1 and oneline.find(onefull) > -1:
					impset.add(onefull)	
	
	return impset

# Get list of paths to java source files for package
def getJavaFiles4Pack(shortpackname):
	return [CompJava.srcPath4JavaFile(shortpackname, simpclass) for simpclass in CompJava.classList4Pack(shortpackname)]
		
def getFullPackList():
	packmap = CompJava.getPackNameMap()
	return [".".join(packmap[packname]) for packname in packmap]
		
def importSet4Pack(shortpackname):
	
	packimp = set()
	
	for onejava in getJavaFiles4Pack(shortpackname):
		#fileimpset = importSet4File(onejava)
		#print "FIle imp is %s" % (fileimpset)
		packimp = packimp.union(importSet4File(onejava))
		#print "PACK imp is %s" % (packimp)
		
	return packimp
		

def getIntraPackRefs4Class(javafile, packlist):
	
	refset = set()
	
	for oneline in open(javafile):
		gimp = [refset.add(onepack) for onepack in packlist if onepack in oneline]
	
	return refset

def checkPackageCycles():
	
	packmap = CompJava.getPackNameMap()
	packrefgraph = GraphImp.GraphImp()
	
	gimp = [packrefgraph.addNode(".".join(packmap[packname])) for packname in packmap]
	
	for simplepack in packmap:
		fullpack = ".".join(packmap[simplepack])
		
		for onejava in getJavaFiles4Pack(simplepack):
			impset = importSet4File(onejava)
			if fullpack in impset:
				impset.remove(fullpack) # remove self-reference here
			
			gimp = [packrefgraph.addEdge(fullpack, pi) for pi in impset]
			
			if packrefgraph.cycleFromNode(fullpack):
				print "ERROR: cycle found after checking file %s" % (onejava)
				print "package is %s, imports are %s" % (fullpack, ",".join(impset))
				sys.exit(1)

		print "%s :: %s" % (fullpack, ",".join(packrefgraph.edgeSet4Node(fullpack)))


	print "Success, Cycle Checker ran okay"




if __name__ == "__main__":

	
	#for simplepack in CompJava.getPackageList():
		
	#	print "File list for package %s is %s" % (simplepack, getJavaFiles4Pack(simplepack))
	
	#for fullpack in getFullPackList():
	#	print "Full package is %s" % (fullpack)
	
	
	#testfile = '/local/src/java/bm_etl/View2Hard.java'
	
	checkPackageCycles()
	
	# TODO: switch back to getPackList
	for onepack in CompJava.getAdnetikPackList():
		#onepack = "slicerep"
		
		# Adhoc is okay to be screwed up
		if "adhoc" in onepack:
			continue
			
		mygraph = GraphImp.GraphImp()
		
		javalist = CompJava.classList4Pack(onepack)
		
		#print "Java list is %s" % (javalist)
		
		# All the simple java classes are nodes in the graph
		gimp = [mygraph.addNode(jclass) for jclass in javalist]
		
		for jclass in sorted(javalist):
			
			if jclass.endswith("Util"):
				continue
			
			#print "Processing Java class %s" % (jclass)
			
			refset = getIntraPackRefs4Class(CompJava.srcPath4JavaFile(onepack, jclass), javalist)
			refset.remove(jclass)
			gimp = [mygraph.addEdge(jclass, oneref) for oneref in refset]
				
			if mygraph.cycleFromNode(jclass):
				
				cyclepath = mygraph.findCycle(jclass)
				print "ERROR, found cycle after adding jclass=%s" % (jclass)
				print "CyclePath is %s" % (",".join(cyclepath))
				sys.exit(1)
				
			#print "Checked %s, refset is \t\t%s" % (jclass, ",".join(refset))
			
		print "package %s checked out okay" % (onepack)


