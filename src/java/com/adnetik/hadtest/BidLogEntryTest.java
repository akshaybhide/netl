/**
 * 
 */
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


public class BidLogEntryTest
{

	public static void main(String[] args) throws Exception
	{
		String filename = "/mnt/adnetik/adnetik-uservervillage/admeld/userver_log/conversion/2012-02-27/2012-02-27-09-30-37.EST.conversion_v14.admeld-rtb-virginia6_15306.log.gz";
		String daycode = "2012-02-25";
		LogType[] touse = new LogType[] {  LogType.conversion };

		for(LogType ltype : touse)
		{
			for(ExcName exc : ExcName.values())
			{
				List<String> exlist = Util.getNfsLogPaths(exc, ltype, daycode);
				
				if(exlist == null)
					{ continue; }
				
				for(String onepath : exlist)
				{
					checkFile(onepath);
				}
			}		
		}					
	}
	
	public static void checkFile(String filename) throws Exception
	{
		BufferedReader bread = Util.getGzipReader(filename);
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String[] toks = oneline.split("\t");
			BidLogEntry ble = new BidLogEntry(LogType.conversion, LogVersion.v14, oneline);		
			ble.strictCheck();
		}		
		
		bread.close();
		
		Util.pf("\nFinished checking file %s", filename);
	}
}
