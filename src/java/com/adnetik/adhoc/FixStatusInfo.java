
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.DbUtil.InfSpooler;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.data_management.*;
import com.adnetik.data_management.BluekaiDataMan.*;

import com.adnetik.analytics.ThirdPartyDataUploader.Party3Db;
import com.adnetik.userindex.*;
import com.adnetik.userindex.UserIndexUtil.*;

// Second pass at the ExelateBilling report
public class FixStatusInfo
{
	
	public static void main(String[] args) throws IOException
	{
		StatusReportMan repman = new StatusReportMan();
			
		String curblock = "2013-01-20";
		
		Set<String> reqset = ListInfoManager.getSing().getPos2NegMap().keySet();
		
		Set<String> compset = Util.treeset();
		
		for(int n = 0; n < 7; n++)
		{
		
			for(String onereq : reqset)
			{
				if(compset.contains(onereq))
					{ continue; }
				
				if(localListReady(onereq, curblock))
				{
					Util.pf("Found list for list req %s, block %s\n", onereq, curblock);	
					compset.add(onereq);
				}
			}
			
			curblock = TimeUtil.nDaysBefore(curblock, 7);
		}
		
		for(String compreq : compset)
		{
			Integer adblistid = ListInfoManager.getSing().getAdbListId(compreq);
			
			/*
			if(adblistid != null)
			{
				repman.sendStatus2AdboardMaybe(compreq, AdbListStatus.completed, adblistid);
			}
			*/
		}
		
		repman.flushInfo();
		
	}
	
	private static boolean localListReady(String listcode, String blockday)
	{
		return (new File(UserIndexUtil.getLocalListPath(blockday, listcode))).exists();
	}	
	
}


