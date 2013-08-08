
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

public class LogErrorCheckMail
{
	private Set<String> _pathSet = Util.treeset();
	
	//Set<String> logPathSet = Util.treeset();
	static Random jRand = new Random();
	
	String _dayCode;
	
	private int _errCount;
	
	Map<String, Integer> errMap = Util.treemap();
	
	SimpleMail _logMail = new SimpleMail("Error Check Daemon Report");
	
	public static void main(String[] args) throws Exception
	{
		ArgMap amap = Util.getClArgMap(args);
		Integer maxfile = amap.getInt("maxfile", Integer.MAX_VALUE);
		
		LogErrorCheckMail checkmail = new LogErrorCheckMail();
		checkmail.run(maxfile);
	}	
	
	public LogErrorCheckMail(String dc)
	{
		Util.massert(TimeUtil.checkDayCode(dc), "Invalid day code %s", dc);
		_dayCode = dc;
		_logMail = new SimpleMail("LogErrorCheckReport for " + _dayCode);
		
		addPaths(LogType.conversion, 1.0D);
		addPaths(LogType.click, 1.0D);
		addPaths(LogType.imp, 1.0D);
		
		addPaths(LogType.bid_all, .01D);
		addPaths(LogType.no_bid_all, .01D);		
	}
	
	public LogErrorCheckMail()
	{
		this(TimeUtil.getYesterdayCode());	
	}
	
	void addPaths(LogType ltype, double addprob)
	{
		int totalcount = 0;
		int prevsize = _pathSet.size();
		
		for(ExcName exc : ExcName.values())
		{
			List<String> plist = Util.getNfsLogPaths(exc, ltype, _dayCode);
			
			if(plist == null)
				{ continue; }
			
			for(String onepath : plist)
			{
				totalcount++;
				
				if(jRand.nextDouble() < addprob)
					{ _pathSet.add(onepath); }
			}
		}
		
		_logMail.pf("\nAdded %d files out of %d total for logtype %s", _pathSet.size()-prevsize, totalcount, ltype);		
	}
	
	
	public void run(int maxfile) throws IOException
	{
		List<String> pathlist = new Vector<String>(_pathSet);
		Collections.shuffle(pathlist);
		
		for(int numscan = 0; numscan < pathlist.size() && numscan < maxfile; numscan++)
		{	
			String onepath = pathlist.get(numscan);
			checkFile(onepath);
			
			if((numscan % 100) == 0)
			{
				_logMail.pf("\nFinished with %d/%d files, found %d errors\n", 
					numscan, pathlist.size(), _errCount);
			}			
			
			// 
			if(_errCount > 100)
			{ 
				_logMail.pf("Found %d errors after scanning %d files, bailing out\n", _errCount, numscan);
				break; 
			}
			
		}
		
		_logMail.sendPlusAdmin("gregg.spivak@digilant.com");
	}
	
	void checkFile(String logfilepath) throws IOException
	{
		PathInfo pinfo = new PathInfo(logfilepath);
			
		BufferedReader bread = Util.getGzipReader(logfilepath);

		int lc = 0;
		int locerr = 0; // errors in just this file

		for(String line = bread.readLine(); line != null; line = bread.readLine())
		{
			lc++;
			
			try {
				BidLogEntry ble = new BidLogEntry(pinfo.pType, pinfo.pVers, line);
				subLogCheck(ble);
			} catch (BidLogFormatException blfex) {
				
				_logMail.pf("Encountered error %s on line %d\n", blfex.e, lc);
				_logMail.pf("File: %s\n", logfilepath);				
				
				// Inc local and global error count
				_errCount++;
				locerr++;
				
				if(locerr > 5)
					{ break; }
			}
		}
		
		bread.close();
	}
	
	private void subLogCheck(BidLogEntry ble) throws BidLogFormatException
	{
		ble.superStrictCheck();
		ble.campaignLineAdIdCheck();
		ble.currencyCodeCheck();
		
		ble.costInfoCheck();
		ble.bidInfoCheck();
		
		// ble.randomErrCheck(0.000001D, jRand);
	}
}
