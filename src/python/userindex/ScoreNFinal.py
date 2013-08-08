#!/usr/bin/python

###################################################
# Runs both the ScoreUser jobs and the FinalListJob
###################################################

import os, sys

sys.path.append("/home/burfoot/src/python/codeutil")
sys.path.append("/home/burfoot/src/python/shared")

import hadJavaCall, ArgMap
	
if __name__ == "__main__":

	argmap = ArgMap.getClArgMap(sys.argv)

	assert argmap.containsKey("blockend"), "Must include explicit block-end date"

	printonly = argmap.getBoolean("printonly", False)
	
	startpart = argmap.getInt("startpart", 0)
	endpart = argmap.getInt("endpart", 24)
	
	print "Printonly is %r" % (printonly)
	
	basearglist = []
	if argmap.containsKey("blockend"):
		basearglist.append("blockend=%s" % (argmap.getString("blockend", "xxxnoval")))
		
	for shardid in range(startpart, endpart):				
		hadJavaCall.runHadoopCall("ScoreUserJob", [str(shardid)] + basearglist, printonly)

	# Final list creation 
	finalclass = "FinalListJob"
	hadJavaCall.runHadoopCall(finalclass, basearglist, printonly)
	
	# Create lift report
	hadJavaCall.runHadoopCall(finalclass, basearglist + ["chart=true"], printonly)
	
	


	
