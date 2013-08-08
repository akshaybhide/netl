
package com.adnetik.userindex;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

import com.adnetik.analytics.*;

public class IterativeInterestScan extends Configured implements Tool
{	
	public static void main(String[] args) throws IOException
	{
		IterativeInterestScan iis = new IterativeInterestScan();
		iis.run(new String[1]);
		
		if("pathlist".equals(args[1]))
		{
			showLogPaths();
		}
	}
	
	public int run(String[] args) throws IOException
	{
		List<String> bigpathlist = Util.vector();
		String daycode = "2011-11-15";

		for(ExcName exc : ExcName.values())
		{
			for(LogType ltype : new LogType[] { LogType.no_bid_all, LogType.bid_all })
			{
				List<String> fpaths = Util.getNfsLogPaths(exc, ltype, daycode);

				if(fpaths != null)
					{ bigpathlist.addAll(fpaths); }
				else
					{ Util.pf("\nFailed to scan log paths for %s, %s, %s", exc, ltype, daycode); }
			}
		}
		
		Util.pf("\nFound %d total log file paths\n", bigpathlist.size());
		
		//processLogFile(bigpathlist.get(0));
		
		sortLocalFile();
		
		return 1;
	}
	
	public static void showLogPaths()
	{
		for(String s : getBigPathList())
		{
			Util.pf("%s\n", s);	
		}
	}
	
	public static List<String> getBigPathList()
	{
		List<String> bigpathlist = Util.vector();
		String daycode = "2011-11-15";

		for(ExcName exc : ExcName.values())
		{
			for(LogType ltype : new LogType[] { LogType.no_bid_all, LogType.bid_all })
			{
				List<String> fpaths = Util.getNfsLogPaths(exc, ltype, daycode);

				if(fpaths != null)
					{ bigpathlist.addAll(fpaths); }
			}
		}
		
		return bigpathlist;
	}
	
	/**
	 * 1) Copy file to local storage, to avoid NFS errors
	 * 2) Strip out all the data that does not have WTP ID set
	 * 3) Sort the remaining data
	 * 4) Call the DoubleScan
	 * 5) Upload to somewhere?
	 */ 
	void processLogFile(String nfspath) throws IOException
	{
		
		String localpath = "/mnt/data/burfoot/interest/local_temp.txt";
		PrintWriter rwriter = new PrintWriter(new File(localpath));
		
		/*
		Util.pf("\nCopying data to local directory...");
		Scanner sc = Util.getGzipScanner(nfspath);
		
		while(sc.hasNextLine())
		{
			String logline = sc.nextLine();
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, logline);
			if(ble == null)
				{ continue; }
			
			String wtp = ble.getField("wtp_user_id").trim();
			
			if(wtp.length() == 0)
				{ continue; }
		
			rwriter.printf("%s\t%s\n", wtp, logline);		
		}
		
		rwriter.close();
		Util.pf(" ... done");
		*/
	}
	
	void sortLocalFile() throws IOException
	{
		String localpath = "/mnt/data/burfoot/interest/local_temp.txt";	
		String sortpath = "/mnt/data/burfoot/interest/sort_local_path.txt";		
		
		String syscall = Util.sprintf("sort %s > %s", localpath, sortpath);
		
		
		Util.syscall(syscall);
		
	}
}
