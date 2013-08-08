
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.BmUtil.*;
import com.digilant.dbh.IABMap;


public class TestNzUpload
{
	// static String NETEZZA_HOST_ADDR = "66.117.49.50";
	
	private static String RSA_FILE_PATH = "/home/burfoot/.ssh/priv_key_342";
	
	private static String LOCAL_TEMP_PATH = "/home/burfoot/netezza/upload_file.tsv";
	
	
	private String _dayCode;
	
	private int _curBatchCount = 0;
	
	private Writer _curWriter;
	
	private SimpleMail _logMail;
	
	public static void main(String[] args) throws Exception
	{
		String daycode = args[0];
		daycode = "yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode;
		
		TestNzUpload tzu = new TestNzUpload(daycode);
		tzu.uploadMany();
		tzu._logMail.send2admin();
	}
	
	public TestNzUpload(String dc)
	{
		TimeUtil.assertValidDayCode(dc);
		
		_dayCode = dc;	
		
		_logMail = new SimpleMail("NZUploadReport for " + _dayCode);
	}
	
	private List<String> getNfsPathList()
	{
		List<String> pathlist = Util.vector();
		
		for(ExcName oneexc : ExcName.values())
		{
			List<String> implist = Util.getNfsLogPaths(oneexc, LogType.imp, _dayCode);
			if(implist != null)
				{ pathlist.addAll(implist); }
		}
		
		return pathlist;
	}
	
	private void uploadMany() throws Exception
	{		
		int fcount = 0;
		
		List<String> pathlist = getNfsPathList();
		
		for(String onepath : pathlist)
		{
			createSubFile(onepath);			
			
			if(_curBatchCount > 1000000)
				{ transferNUpload(); }
			
			fcount++;
			
			if((fcount % 25) == 0)
			{
				_logMail.pf("Finished scanning file %d/%d, batchcount is %d\n", fcount, pathlist.size(), _curBatchCount);	
			}
			
			// if(fcount > 40)
			//	{ break; }
		}
		
		transferNUpload();
	}
	
	private void openIfNecessary() throws IOException
	{
		if(_curWriter == null)
			{ _curWriter = FileUtils.getWriter(LOCAL_TEMP_PATH); }
	}
	
	private void transferNUpload() throws IOException
	{
		if(_curWriter != null)
		{
			_curWriter.close();
			_curWriter = null;
		}
		
		
		File upfile = new File(LOCAL_TEMP_PATH);
		transferFile(upfile, _logMail);
		remoteUploadCommand(LOCAL_TEMP_PATH);
		
		_curBatchCount = 0;
	}
	
	private void createSubFile(String nfslogpath) throws Exception
	{
		Random jrand = new Random();
		BufferedReader bread = FileUtils.getGzipReader(nfslogpath);
		
		// Open the writer object
		openIfNecessary();
		int onefilecount = 0;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			BidLogEntry ble = new BidLogEntry(LogType.imp, LogVersion.v21, oneline);
			
			// Util.pf("Campaign ID is %d, wtp id is %s\n", 
			// 	ble.getIntField(LogField.campaign_id), ble.getField(LogField.wtp_user_id));
			
			List<String> onerow = Util.vector();
			onerow.add(ble.getField(LogField.date_time));
			onerow.add(_dayCode);
			
			add2row(onerow, ble, LogField.wtp_user_id, 40);
			add2row(onerow, ble, LogField.transaction_id, 40);
			add2row(onerow, ble, LogField.ad_exchange, 15);
			add2row(onerow, ble, LogField.campaign_id, 10);
			add2row(onerow, ble, LogField.line_item_id, 20);
			add2row(onerow, ble, LogField.size, 10);
			add2row(onerow, ble, LogField.utw, 10);
			add2row(onerow, ble, LogField.currency, 3);
			add2row(onerow, ble, LogField.language, 20);
			add2row(onerow, ble, LogField.domain, 100);
			add2row(onerow, ble, LogField.user_country, 2);
			add2row(onerow, ble, LogField.user_region, 2);
			add2row(onerow, ble, LogField.user_city, 100);
			add2row(onerow, ble, LogField.user_postal, 15);
			add2row(onerow, ble, LogField.bid, 20);
			add2row(onerow, ble, LogField.winner_price, 20);
			onerow.add(jrand.nextInt(1000)+"");			
			//add2row(onerow, ble, IABMap.getCategoryLogField(Util.getExchange(nfslogpath)), 100);
			// String row = Util.join(onerow, "\t");
			// String row = Util.sprintf("%d\t%s", ble.getIntField(LogField.campaign_id), ble.getField(LogField.wtp_user_id));
			///rowlist.add(row);
			
			FileUtils.writeRow(_curWriter, "\t", onerow.toArray());
			_curBatchCount++;
			onefilecount++;
		}
		
		bread.close();
		
		// FileUtils.writeFileLinesE(rowlist, targfile.getAbsolutePath());
		// Util.pf("Scanned %d log records, batch size now %d\n", onefilecount, _curBatchCount);
	}
	
	
	static void add2row(List<String> onerow, BidLogEntry ble, LogField lf, int maxsize)
	{
		String fval = ble.getField(lf);
		fval = (fval.length() > maxsize ? fval.substring(0, maxsize) : fval);
		onerow.add(fval);
	}
	
	static void transferFile(File localfile, SimpleMail logmail) throws IOException
	{
		transferFile(localfile, localfile.getAbsolutePath(), logmail);
	}
	
	static void transferFile(File localfile, String rempath, SimpleMail logmail) throws IOException
	{
		Util.massert(localfile.exists());
		
		String syscall = Util.sprintf("scp -i %s %s %s:%s", 
			RSA_FILE_PATH, localfile.getAbsolutePath(), 
			DbUtil.NZConnSource.NZ_HOST_ADDR, rempath);
		
		// Util.pf("SYSCALL is %s\n", syscall);
		
		List<String> outlist = Util.vector();
		List<String> errlist = Util.vector();
		
		logmail.pf("Initiating SCP transfer... ");
		double startup = Util.curtime();
		Util.syscall(syscall, outlist, errlist);
		logmail.pf(" ... done, took %.03f seconds\n", (Util.curtime()-startup)/1000);
		
	}
	
	
	private void remoteUploadCommand(String remnzpath) throws IOException
	{
		remoteUploadCommand(remnzpath, "imp_test_1", _logMail);
	}
	
	static void remoteUploadCommand(String remnzpath, String tabname, SimpleMail logmail) throws IOException
	{
		String nzupload = Util.sprintf("/nz/kit.7.0/bin/nzload -u burfoot -db fastetl -t %s -pw data_101? -df %s", tabname, remnzpath);
		
		String sshsyscall = Util.sprintf("ssh -i %s %s %s",
			RSA_FILE_PATH, DbUtil.NZConnSource.NZ_HOST_ADDR, nzupload);
		
		// String sshsyscall = " ssh -i /home/burfoot/.ssh/priv_key_342 66.117.49.50 /nz/kit.7.0/bin/nzload -u burfoot -db fastetl -t dcbtest -pw data_101? -df dcbtest.tsv
		
		logmail.pf("Initiating disk-NZ upload... ");
			
			
		List<String> outlist = Util.vector();
		List<String> errlist = Util.vector();
		
		double startup = Util.curtime();
		Util.syscall(sshsyscall, outlist, errlist);
		
		logmail.pf(" ... done, %d output lines, %d error lines, took %.03f\n", 
			outlist.size(), errlist.size(), (Util.curtime()-startup)/1000);
		
		for(String oneerr : errlist)
		{
			logmail.pf("ERROR: %s\n", oneerr);
		}
	}	
	
	
}
