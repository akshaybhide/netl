#!/usr/bin/python

import re, os, sys, fileinput

# TODO: this is not a good way to do this
sys.path.append("/home/burfoot/src/python/codeutil")
sys.path.append("/home/burfoot/src/python/shared")

import hadJavaCall, ArgMap

if __name__ == "__main__":

	argmap = ArgMap.getClArgMap(sys.argv)
	assert argmap.containsKey("blockend"), "Must specify blockend"
	
	print "Argmap is %s" % (argmap)

	precompclass = "UserIndexPrecompute"
	learnclass = "LearningTool"
	
	hadJavaCall.runHadoopCall(precompclass, sys.argv[1:])
	hadJavaCall.runHadoopCall(learnclass, sys.argv[1:])
