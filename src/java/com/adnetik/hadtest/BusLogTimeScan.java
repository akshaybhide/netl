
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;


// Test how long it takes to count the lines for every BigLog file.
public class BusLogTimeScan
{

	public static void main(String[] args) throws Exception
	{

		for(ExcName oneexc : ExcName.values())
		{
			
			List<String> nfspaths = Util.getNfsLogPaths(oneexc, LogType.imp, "2012-01-03");
			
			if(nfspaths == null)
			{ 
				System.err.printf("\nFound no paths for %s", oneexc.toString()); 
				continue;
			}
			
			for(String onenfs : nfspaths)
			{
				//Util.pf("\nOne path is %s", onenfs);	
				//countLines(onenfs);
				countSize(onenfs, oneexc.toString());
			}		
		}
	}
	
	
	
	static void countSize(String path, String exchange)
	{
		long flen = (new File(path)).length();
		Util.pf("%s\t%s\t%d\n", path, exchange, flen);
	}
	
	static void countLines(String path) throws IOException
	{
		long flen = (new File(path)).length();
		
		BufferedReader bread = Util.getGzipReader(path);
		int lcount = 0;
		
		double start = System.currentTimeMillis();
		
		for(String line = bread.readLine(); line != null; line = bread.readLine())
		{
			lcount++;	
		}
		
		double end = System.currentTimeMillis();
		
		double timesec = (end-start)/1000;
		
		Util.pf("\nRead file \n\t%s\n\tLines = %d, Time = %.03f secs, Length=%d, Rate=%.03f bytes/sec", 
			path, lcount, timesec, flen, ((double) flen)/timesec);
	}
	
	
}
