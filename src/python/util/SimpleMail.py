#!/usr/bin/python

import os,random,time,datetime

ADMIN_LIST = []
#ADMIN_LIST.append("daniel.burfoot@gmail.com")
ADMIN_LIST.append("daniel.burfoot@digilant.com")

def myTimestamp():
	now = datetime.datetime.now()
	return now.strftime("%m-%d %H:%M:%S")

class SimpleMail:
	
	subject = ""
	messgList = []
	
	def __init__(self, subj):
		
		print "Building a SimpleMail"
		self.subject = subj
	
	def readLogData(self, path):
		
		for logline in open(path):
			self.messgList.append(logline.strip())
	
		
	def addLogLine(self, line):
		
		ts = myTimestamp()
		m = "%s : %s" % (ts, line)
		print "Message is %s" % (m)
		self.messgList.append(m)
		
	def send2admin(self):
		self.send2list(ADMIN_LIST)		
		
	def send2list(self, rlist):
		for recip in rlist:
			self.send(recip)
		
	def send(self, recip):
		
		tempfile = self.gimmeTemp()
		
		fhandle = open(tempfile, 'w')
		for aline in self.messgList:
			fhandle.write(aline + "\n")
		fhandle.close()
		
		subjline = "[AdminMail] %s" % (self.subject)
		mailcall = "cat %s | mail -s \"%s\" %s" % (tempfile, subjline, recip)
		print "Mailcall is %s" % (mailcall)
		
		os.system(mailcall)
		os.remove(tempfile)
		
	def gimmeTemp(self):
		
		tempid = int(random.random() * 1000000)
		return "/tmp/tempmail_%d" % (tempid)
		
if __name__ == "__main__":


	smail = SimpleMail("we're looking for a ... ")
	smail.addLogLine("a WHAT?")
	
	time.sleep(10)
	smail.addLogLine("a GRRRAIILL...?")

	smail.send2admin()
	
	
	
