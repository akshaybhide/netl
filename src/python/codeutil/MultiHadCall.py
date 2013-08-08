#!/usr/bin/python

import re, os, sys, fileinput, JavaCall, hadJavaCall
			
sys.path.append('/local/src/python/shared')

import Util
			
if __name__ == "__main__":

	if len(sys.argv) < 3:
		print "MultiHadCall <jclass> <arglist.txt>"
		sys.exit(1)
		
	jclass = sys.argv[1]
	
	fullclass = JavaCall.getFullClass(jclass)
	
	arglist = [onearg.strip() for onearg in open(sys.argv[2])]
	
	
	
	if Util.promptOkay("Going to run %s for %s?" % (fullclass, ",".join(arglist))):
		
		for onearg in arglist:
			
			moreargs = [onearg]
			
			if len(sys.argv) > 3:
				moreargs.extend(sys.argv[3:])			
				
			hadJavaCall.runHadoopCall(jclass, moreargs)
