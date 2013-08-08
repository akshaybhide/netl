package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

// This is the "dumb" method of pulling AdBoard data to the reporting DB.
// All it does is to do a DB mysqldump and upload.
public class AdBoardPull
{		
	
	private DbTarget _dbTarg; 

	public static final String ADBOARD_DB_NAME = "adnetik";
	
	// Okay, there are lots of credentials going on here.
	// 	
	public static String BURFOOT_RSA_PATH = "/home/burfoot/.ssh/priv_key_342";
	
	private static String DUMP_FILE = "/home/burfoot/bm_etl/adboardpull/dumpfile.txt";
	private static String TEST_DUMP_FILE = "/home/burfoot/bm_etl/adboardpull/test_dump.txt";
	
	private static String ADBOARD_CRED_FILE = "/home/burfoot/bm_etl/adboardpull/mysqladboarddumper.txt";
	private static String ADREPORT_CRED_FILE = "/home/burfoot/bm_etl/adboardpull/adreportcred.txt";
	
	static boolean isTest;
	boolean hitError = false;
	String dayCode;
	
	private SimpleMail _logMail;
	
	private AdBoardMachine _adMachine = AdBoardMachine.prod;
	
	public static void main(String[] args) throws Exception
	{
		ArgMap argmap = Util.getClArgMap(args);
		DbTarget dbtarg = DbTarget.valueOf(argmap.getString("dbtarg", "external"));
		
		AdBoardPull abp = new AdBoardPull(false, dbtarg);
		abp._logMail.pf("%s\n", Util.BIG_PRINT_BAR);
		abp._logMail.pf("Running adboard pull for %s\n", abp.dayCode);		
		abp.runProcess();
		abp._logMail.pf("Finished adboard pull for %s\n", abp.dayCode);		
		abp._logMail.pf("%s\n", Util.BIG_PRINT_BAR);	
		
		abp._logMail.send2admin();
	}
	
	public AdBoardPull(boolean it, DbTarget dbt)
	{
		isTest = it;
		dayCode = TimeUtil.getTodayCode();	
		_dbTarg = dbt;
		
		_logMail = new DayLogMail(this, dayCode);
	}
	
	void runProcess() throws Exception
	{
		adboard2dump();
		
		if(hitError)
		{ 
			_logMail.pf("Encountered errors with adboard2dump operation, bailing out\n");
			return;
		}
		
		dump2adreport();	
		
		if(hitError)
		{ 
			_logMail.pf("Encountered errors with dump2adreport operation, bailing out\n");
			return;
		}		
	}
	
	String getDumpFile()
	{
		return (isTest ? TEST_DUMP_FILE : DUMP_FILE);
	}	
	
	public static String getAdBoardRsaPath()
	{
		String username = Util.getUserName();
		String rsapath = Util.sprintf("/home/%s/.ssh/adboard_id_rsa", username);
		Util.massert((new File(rsapath)).exists(),
			"AdBoard RSA file %s does not exist, please put it there", rsapath);
		
		return rsapath;		
	}
		
	// Creds for Mysql on Adboard machine
	public static String[] getAdboardMysqlCreds()
	{
		return getCredPair(ADBOARD_CRED_FILE);
	}
	
	// Mysql username/password pair
	static String[] getAdReportMysqlCreds()
	{
		return getCredPair(ADREPORT_CRED_FILE);
	}
	
	private static String[] getCredPair(String path2cred)
	{
		List<String> credlist = FileUtils.readFileLinesE(path2cred);	
		return new String[] { credlist.get(0), credlist.get(1) };		
	}
	
	static String getMysqlDumpCall()
	{
		String[] adrepcred = getAdboardMysqlCreds();
		return Util.sprintf(" mysqldump -u %s -p%s %s ", adrepcred[0], adrepcred[1], ADBOARD_DB_NAME);
	}
	
	public static String getMysqlPushCall(DbTarget dbtarg)
	{
		String[] arepcred = getAdReportMysqlCreds();
		
		// TODO: should use dbtarg.getHostName()
		String hostname = DatabaseBridge.getIpDbName(dbtarg)[0];
		
		return Util.sprintf("mysql -u %s -p%s -h %s %s", 
			arepcred[0], arepcred[1], hostname, ADBOARD_DB_NAME);		
	}
	
	// Grabs the data from AdBoard, saves it to the dump file
	void adboard2dump() throws Exception
	{
		double startup = Util.curtime();
		int lcount = 0;
		
		// Default username and priv-key-path are set automatically		
		Util.SshUtil sshut = new Util.SshUtil();
		{
			String[] host_db = DatabaseBridge.getIpDbName(_dbTarg);	
			sshut.hostname = _adMachine.getHostName();
			sshut.rsapath = getAdBoardRsaPath();
		}		
		
		String dumpcall = AdBoardPull.getMysqlDumpCall();		
		String syscall = sshut.getSysCall(dumpcall);
		
		// abp._logMail.pf("
		
		// run the Unix "ps -ef" command
		// using the Runtime exec method:
		Process p = Runtime.getRuntime().exec(syscall);
	
		PrintWriter pwrite = new PrintWriter(getDumpFile());
		BufferedReader bread = new BufferedReader(new InputStreamReader(p.getInputStream()));
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			pwrite.write(oneline);
			pwrite.write("\n");
			lcount++;
			
			if(isTest && lcount >= 500)
				{ break; }
		}
		bread.close();
		pwrite.close();
		_logMail.pf("Read %d lines from dump file, took %.03f secs\n", lcount, (Util.curtime()-startup)/1000);
		
		BufferedReader readerr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		for(String oneline = readerr.readLine(); oneline != null; oneline = readerr.readLine())
		{
			Util.pf("ERROR: %s\n", oneline);
			hitError = true;
		}
		readerr.close();
	}
	
	// TODO: change this and others to use List<String> arguments
	// sent to a common syscall method
	void dump2adreport() throws Exception
	{	
		String syscall = getMysqlPushCall(_dbTarg);
				
		// Util.pf("Syscall is \n\t%s\n", syscall);
		
		Process p = Runtime.getRuntime().exec(syscall);	
		{
			double startup = Util.curtime();
			int lcount = 0;
			PrintWriter sendto = new PrintWriter(p.getOutputStream());
			BufferedReader dumpin = Util.getReader(getDumpFile());

			for(String oneline = dumpin.readLine(); oneline != null; oneline = dumpin.readLine())
			{ 
				sendto.write(oneline);
				sendto.write("\n");
				
				lcount++;
				if((lcount % 300) == 0)
					{ _logMail.pf("\tFinished sending line %d\n", lcount); }
			}

			dumpin.close();
			sendto.close();
			_logMail.pf("Sent %d lines total, took %.03f secs\n", lcount, (Util.curtime()-startup)/1000);			
		}
	}
}
