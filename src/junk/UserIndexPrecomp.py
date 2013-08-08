#!/usr/bin/python

###################################################
# Basically a wrapper around code that does the precomputation
# of feature responses for the UserIndex.
# This code runs as user "burfoot"
###################################################

import os, sys
import SynchUtil

MAX_USER = 30000

def map2set(mymap):
	
	aset = set()
	
	for key in mymap:
		aset.add(key)
		aset.add(mymap[key])
		
	return aset
	
def getPixelLogPath(pixid, opcode):
	
	logdir = "/mnt/data/userindex/%s/" % (SynchUtil.get_today())
	if not os.path.exists(logdir):
		os.system("mkdir %s" % (logdir))

	logpath = "%slog_%s_%s.txt" % (logdir, opcode, pixid)	
	return logpath
	
def genUserReport(pospix, negpix):
	
	logpath = getPixelLogPath(pospix, "genoutput")

	report_call = "java -cp /mnt/jars/adnetik.jar com.adnetik.userindex.UIndexDataManager genoutput %s %s > %s" % (pospix, negpix, logpath)
	print report_call	
	os.system(report_call)

def autoLearn(pospix, negpix):
	
	logpath = getPixelLogPath(pospix, "learn")

	learn_call = "java -cp /mnt/jars/adnetik.jar com.adnetik.userindex.UIndexDataManager learn %s %s > %s" % (pospix, negpix, logpath)
	print learn_call	
	os.system(learn_call)

def precompPix(pixid):

	logpath = getPixelLogPath(pixid, "precomp")

	pc_call = "hadoop jar /mnt/jars/adnetik.jar com.adnetik.userindex.FeatureReport precomp %s %d > %s" % (pixid, MAX_USER, logpath)
	print pc_call	
	os.system(pc_call)
	
if __name__ == "__main__":
	
	pix_comp_map = {}
	#pix_comp_map["click_camp_1408"] = "negative_US_000" # HP
	#pix_comp_map["click_camp_1396"] = "negative_US_000" # HP
	#pix_comp_map["click_camp_1339"] = "negative_US_000" # HP
	pix_comp_map["pixel_7731"] = "negative_US_000" # ?? iRobot
	#pix_comp_map["goodway"] = "negative_US_000" 
	pix_comp_map["pixel_5308"] = "negative_US_000" # HP
	
	targlist = []
	
	if len(sys.argv) == 2:
		targpix = int(sys.argv[1])
		assert targpix in pix_comp_map
		targlist.append(targpix)
	else:	
		targlist.extend([onepix for onepix in pix_comp_map])

	print "Pixel target list is %s" % (targlist)

	# This is kind of the hacky way to do things
	sys.path.append("/mnt/src/python/util")
	import SimpleMail
	logmail = SimpleMail.SimpleMail("UIndex Precomp")
	
	pc_set = set()
	for x in targlist:
		pc_set.add(x)
		pc_set.add(pix_comp_map[x])

	for onepix in pc_set:
		
		precompPix(onepix)
		logmail.addLogLine("Finished precomputation for pixel %s" % (onepix))
		
	for onepix in targlist:
		genUserReport(onepix, pix_comp_map[onepix])
		logmail.addLogLine("Finished user report for pixel comparison %s - %s" % (onepix, pix_comp_map[onepix]))
		
		#autoLearn(onepix, pix_comp_map[onepix])
		#logmail.addLogLine("Finished AdaBoost learning for pixel comparison %s - %s" % (onepix, pix_comp_map[onepix]))
			
			
	logmail.send2admin()

