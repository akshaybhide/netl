#!/usr/bin/python

import re, os, sys, fileinput
	
GIMP_TSV_PATH = "/home/burfoot/userindex/mailtest/gimp_file.tsv"	
GIMP_SQL_PATH = "/home/burfoot/userindex/mailtest/gimp_file.sql"	

RESULT_FILE = "/home/burfoot/userindex/mailtest/result.csv"
	
def sendMail2People():
	
	syscall = "echo \"Audience Index Status\" | mutt -a %s -s \"AIMonitorMailReport\" -- daniel.burfoot@digilant.com" % (RESULT_FILE)
	#echo "Daily dCPM report campaign 1910" | mutt -a  /home/amol/dcpm_1910.csv  -s "Daily dCPM report campaign Subzero" -- luba.kogan@digilant.com	
	os.system(syscall)
	
def countMapFromSql(sql):

	fhandle = open(GIMP_SQL_PATH, 'w')
	fhandle.write(sql)
	fhandle.close()

	syscall = "mysql --skip-column-names -u burfoot -h thorin-internal.adnetik.com -pdata_101? < %s > %s" % (GIMP_SQL_PATH, GIMP_TSV_PATH)
	os.system(syscall)
	
	resmap = {}
	for oneline in open(GIMP_TSV_PATH):
		(daycode, count) = oneline.strip().split("\t")
		assert not daycode in resmap
		resmap[daycode] = int(count)

	return resmap

def sql2Disk(sql, outputfile):
	
	fhandle = open(GIMP_PATH, 'w')
	fhandle.write(sql)
	fhandle.close()
	
	syscall = "mysql -u burfoot -h thorin-internal.adnetik.com -pdata_101? < %s | sed 's/\t/,/g' >> %s" % (GIMP_PATH, outputfile)

	#print "SYS call is %s" % (syscall)
	os.system(syscall)

def getSqlQuery(tabname):
	
	return "SELECT RI.can_day, count(*) as %s_rcount from userindex.%s BASET join userindex.report_info RI on BASET.report_id = RI.report_id GROUP BY RI.can_day ORDER BY can_day desc LIMIT 15" % (tabname, tabname)
		
if __name__ == "__main__":

	"""
	Run three queries against the UserIndex DB 
	Make sure there is data for the right times
	"""
	
	os.system("echo 'Table Counts' > %s" % (RESULT_FILE))
	
	tablist = ["feature_table", "lift_report", "eval_scheme", "party3_report"]
	
	resmapmap = {}
	
	for onetab in tablist:
		onesql = getSqlQuery(onetab)
		resmap = countMapFromSql(onesql)
		resmapmap[onetab] = resmap
		
	dayset = set()
	for onetab in tablist:
		gimp = [dayset.add(daycode) for daycode in resmapmap[onetab]]

	# Ugh, who said Python was nicer than Java?
	daylist = sorted(dayset)
	daylist.reverse()
	
	rowlines = []
	reclines = []

	reclines.append(["date"] + tablist)

	for oneday in daylist:
			
		rowdata = [oneday]
		
		for onetab in tablist:
			countval = 0
			try:
				countval = resmapmap[onetab][oneday]
			except:
				pass
		
			rowdata.append(str(countval))
	
		reclines.append(rowdata)
		
		
	csvout = open(RESULT_FILE, 'w')
	for onerec in reclines:
		csvout.write("%s\n" % (",".join(onerec)))
	csvout.close()
	
	sendMail2People()
