
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

import com.adnetik.userindex.UserIndexUtil.*;


public class IpLookup implements BidLogListener
{		
	public static final int LOOKUP_LIST_SIZE = 12524970;
	
	public static final String LOOKUP_LIST_PATH = "/home/burfoot/acquireweb/ToCharlieCommercialIPs.txt";
	
	private long[] _lookupList;
	
	private int _badIps = 0;
	
	private int _foundTargs = 0;
	
	public IpLookup()
	{
		// Need zero-arg constructor	
		
		
	}
	
	public void inspect(BidLogEntry ble)
	{
		String user_ip = ble.getField("user_ip");
		long ip_long = Util.ip2long(user_ip);
		
		try { ip_long = Util.ip2long(user_ip); }
		catch (Exception ex) { 
			
			Util.pf("Found badly formatted IP: " + user_ip);
			_badIps++;
		}
		
		int lookup = Arrays.binarySearch(_lookupList, ip_long);
		
		if(lookup >= 0)
		{
			// Util.pf("Found target ip %s\n", user_ip);	
			_foundTargs++;
			
			logHitInformation(ble);
		}
		
		
	}
	
	// This is going to do whatever it is we want to do.
	private void logHitInformation(BidLogEntry ble)
	{
		
		
		
	}
	
	public void initialize()
	{
		try {
			double startup = Util.curtime();
			_lookupList = new long[LOOKUP_LIST_SIZE];
			int lcount = 0;
			long previp = -1;
			
			BufferedReader bread = FileUtils.getReader(LOOKUP_LIST_PATH);
			{
				String dummy = bread.readLine();
				Util.massert(dummy.indexOf("ip") > -1);
			}
			
			// This will break if the number of lines in the file is wrong
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				long iptarg = Util.ip2long(oneline.trim());
				_lookupList[lcount++] = iptarg;
				
				Util.massert(previp < iptarg, "IPs listed out of prev=%d, new=%d", previp, iptarg);
				previp = iptarg;
			}
			bread.close();
			Util.pf("Finished reading data file, took %.03f secs\n", (Util.curtime()-startup)/1000);
			
		} catch (IOException ioex) {
			
			throw new RuntimeException(ioex);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		IpLookup iplook = new IpLookup();
		iplook.initialize();
		
		Util.showMemoryInfo();
		
		int tlinecount = 0;
		
		
		List<String> pathlist = Util.getNfsLogPaths(ExcName.admeld, LogType.bid_all, "2012-09-12");
		for(String onepath : pathlist)
		{
			Util.pf("Scanning file %s\n", onepath);
			PathInfo pinfo = new PathInfo(onepath);
			BufferedReader bread = Util.getGzipReader(onepath);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				BidLogEntry ble = new BidLogEntry(pinfo.pType, pinfo.pVers, oneline);
				iplook.inspect(ble);
				tlinecount++;
			}
			bread.close();
			
			Util.pf("Found %d targets out of %d lines\n", iplook._foundTargs, tlinecount);
		}
		
	}
}
