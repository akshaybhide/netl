
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

public class ErrorCheckDaemon
{
	Map<LogType, Set<String>> logPathMap = Util.treemap();
	
	//Set<String> logPathSet = Util.treeset();
	static Random jRand = new Random();
	
	String dayCode;
	
	Map<String, Integer> errMap = Util.treemap();
	
	SimpleMail logmail = new SimpleMail("Error Check Daemon Report");
	
	public ErrorCheckDaemon(String dc)
	{
		dayCode = dc;	
	}
	
	public ErrorCheckDaemon()
	{
		this(TimeUtil.getYesterdayCode());	
	}
	
	public static void main(String[] args) throws Exception
	{
		ErrorCheckDaemon ecd = new ErrorCheckDaemon();
		
		ecd.addPaths(LogType.conversion, 1.0D);
		ecd.addPaths(LogType.click, 1.0D);
		ecd.addPaths(LogType.imp, 1.0D);
		ecd.run();
		
		ecd.addPaths(LogType.bid_all, .01D);
		ecd.addPaths(LogType.no_bid_all, .01D);
		
		ecd.run();
	}

	void addPaths(LogType ltype, double addprob)
	{
		int totalcount = 0;
		
		for(ExcName exc : ExcName.values())
		{
			List<String> plist = Util.getNfsLogPaths(exc, ltype, dayCode);
			
			if(plist == null)
				{ continue; }
			
			for(String onepath : plist)
			{
				totalcount++;
				
				if(jRand.nextDouble() < addprob)
				{ 
					Util.setdefault(logPathMap, ltype, new TreeSet<String>());
					logPathMap.get(ltype).add(onepath);
				}
			}
		}
		
		messagePf("\nAdded %d files out of %d total for logtype %s", logPathMap.get(ltype).size(), totalcount, ltype);		
	}
	
	
	public void run()
	{
		
		for(LogType ltype : logPathMap.keySet())
		{
			int numscan = 0;			
			Set<String> logPathSet = logPathMap.get(ltype);
			messagePf("Running for log type %s", ltype);
			
			
			while(!logPathSet.isEmpty())
			{
				String logfilepath = getSinglePath(logPathSet);
				//Util.pf("\nChecking file %s", logfilepath);
				
				try { 
					checkFile(ltype, logfilepath);
				} catch (IOException ioex) {
					
					messagePf("\nError reading file %s", logfilepath); 
				}
				
				logPathSet.remove(logfilepath);
				numscan++;
				
				if((numscan % 100) == 0)
				{
					messagePf("\nFinished with %d files for type %s, %d remaining, found %d errors", 
						numscan, ltype, logPathSet.size(), errMap.size());
				}
			}
			
			messagePf("Scanned %d files for log type %s", numscan, ltype);
		}
		
		logmail.send2admin();
	}
	
	void checkFile(LogType reltype, String logfilepath) throws IOException
	{
		LogVersion relvers = Util.fileVersionFromPath(logfilepath);
		
		BufferedReader bread = Util.getGzipReader(logfilepath);

		int errcount = 0;
		int lc = 0;

		for(String line = bread.readLine(); line != null; line = bread.readLine())
		{
			lc++;
			
			//Util.pf("\nLine is %s", line);
			
			try {
				BidLogEntry ble = new BidLogEntry(reltype, LogVersion.v13, line);
				ble.superStrictCheck();
			} catch (BidLogFormatException blfex) {
				
				if(errcount < 5)
				{
					messagePf("\nEncountered error %s on line %d", blfex.e, lc);
					messagePf("\nLine is:\n%s", line);
				}
				errcount++;
			}
			
			if((lc % 10000) == 0)
			{
				//Util.pf("\nFinished scanning line %d", lc);	
			}
		}
		
		if(errcount > 0)
		{
			errMap.put(logfilepath, errcount);
			writeHitData();
		}
		
		
		if(errcount > 0)
		{
			messagePf("\nScanned %d lines, found %d errors for %s file", lc, errcount, reltype);		
		}
	}
	
	void writeHitData() throws IOException
	{
		String savepath = Util.sprintf("/mnt/data/burfoot/errorcheck/saveinfo_%s.csv", dayCode);
		PrintWriter pwrite = new PrintWriter(savepath);
		
		for(String filename : errMap.keySet())
		{
			pwrite.printf("\n%s\t%d", filename, errMap.get(filename));
		}
		
		pwrite.close();
	}
	
	public static String getSinglePath(Set<String> logpathset)
	{
		List<String> gimp = Util.vector();
		gimp.addAll(logpathset);
		String onepath = gimp.get(jRand.nextInt(gimp.size()));
		return onepath;
	}
	
	public static LogType getLogType(String logpath)
	{
		return LogType.no_bid_all;
	}
	
	void messagePf(String format, Object... args)
	{
		String mssg = Util.sprintf(format, args);
		logmail.addLogLine(mssg.trim());
		Util.pf(format, args);
	}
	
}
