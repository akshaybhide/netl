#!/usr/bin/python

import re, os, sys, fileinput

import Util

class QueryBatch:
	
	_argDir = ""
	_dayCode = ""
	_campSet = set()
	_queryList = []
	
	def initData(self, adir, dc, cset):
		self._argDir = adir
		self._dayCode = dc
		self._campSet = cset
		self.popQueryCodeList()
		self.createOutputDir()
		print "Found %d query codes: %s" % (len(self._queryList), ",".join(self._queryList))

	def createOutputDir(self):
		
		outputdir = "%s/output/%s/" % (self._argDir, self._dayCode)
		if not os.path.exists(outputdir):
			os.mkdir(outputdir)
			print "Created output directory %s" % (outputdir)

	def getStageFile(self):
		return "%s/stage.st_sql" % (self._argDir)

	def getCsvOutputFile(self, sqlcode):
		return "%s/output/%s/%s.csv" % (self._argDir, self._dayCode, sqlcode)
		
	def runMysqlCall(self, sqlcode):
		stagefile = self.getStageFile()
		csvout = self.getCsvOutputFile(sqlcode)
		
		syscall = "mysql -u burfoot -h thorin-internal.adnetik.com -pdata_101? < %s | sed s/'\t'/,/g > %s" % (stagefile, csvout)
		print "Syscall is %s" % (syscall)
		os.system(syscall)
		
	def swap2stageSql(self, sqlcode):
		daysed = "sed s/xxDAYCODExx/%s/" % (self._dayCode)	
		cmpsed = "sed s/xxCAMPSETxx/%s/" % (",".join([str(cid) for cid in self._campSet]))
		
		syscall = "cat %s/%s.sql | %s | %s > %s" % (self._argDir, sqlcode, daysed, cmpsed, self.getStageFile())
	
		print "System call is %s" % (syscall)
		os.system(syscall)
	
	def popQueryCodeList(self):
		for onefile in os.listdir(argdir):
			if onefile.endswith(".sql"):
				self._queryList.append(onefile[:-4])
			
	def proc4Code(self, sqlcode):
		self.swap2stageSql(sqlcode)
		self.runMysqlCall(sqlcode)
		self.mailResult(sqlcode)


	def mailResult(self, sqlcode):
		csvfile = self.getCsvOutputFile(sqlcode)
		muttcall = "echo \"Results from QueryCode %s\" | mutt -a %s -s \"%sReport\" -- daniel.burfoot@digilant.com" % (sqlcode, csvfile, sqlcode)
		os.system(muttcall)
		
		#echo "Daily dCPM report campaign 1910" | mutt -a  /home/amol/dcpm_1910.csv  -s "Daily dCPM report campaign Subzero" -- luba.kogan@digilant.com
if __name__ == "__main__":

	"""
	Input is sequence of simple strings (e.g. domains).
	Output is sorted map of counts of those strings
	"""
	
	if len(sys.argv) < 4:
		print "Usage: QueryMailer.py <argdir> <campset> <daycode>"
		sys.exit(1)
	
	argdir = sys.argv[1]
	assert os.path.exists(argdir), "Could not find arg directory %s" % (argdir)
	
	campset = set()
	for campid in sys.argv[2].split(","):
		campset.add(int(campid))

	daycode = sys.argv[3]
	if daycode == "yest":
		daycode = Util.get_yesterday()

	
	qbatch = QueryBatch()
	qbatch.initData(argdir, daycode, campset)

	#qbatch.procAll()
	qbatch.proc4Code("fast_domainvsfast_general")

