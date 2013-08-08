#!/usr/bin/python

import sys, os
			
			
def getMysqlCall(dbcode):
	
	hostname = ""
	username = "burfoot"
	password = "data_101?"
	
	if dbcode == "thor-int":
		hostname = "thorin-internal.adnetik.com"
	elif dbcode == "thorin":
		hostname = "thorin.adnetik.com"
	else:
		raise "Unknown DB code: %s" % (dbcode)
		
	return "mysql --skip-column-names -h %s -u %s -p%s " % (hostname, username, password)
		
def sendCommandList(commlist, hostname):
	
	# TODO: need a random gimp location file
	gimppath = "gimp_2u3434.txt"

	fhandle = open(gimppath, 'w')
	
	for onecomm in commlist:
		fhandle.write(onecomm)
		fhandle.write("\n")
	
	fhandle.close()	

	sqlcall = "%s < %s" % (getMysqlCall(hostcode), gimppath)

	os.system(sqlcall)
	
if __name__ == "__main__":

	"""
	Send list of SQL queries to the server
	"""
	
	hostcode = sys.argv[1]	
	numbatch = int(sys.argv[2]) if len(sys.argv) > 2 else 1
		
	commlist = []	
		
	for oneline in sys.stdin:	
		commlist.append(oneline)
		
		if len(commlist) >= numbatch:
			sendCommandList(commlist, hostcode)
			commlist = []
			
	if len(commlist) > 0:
		sendCommandList(commlist, hostcode)
		commlist = []
		
