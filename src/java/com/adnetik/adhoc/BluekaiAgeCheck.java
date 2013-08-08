
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

// Second pass at the ExelateBilling report
public class BluekaiAgeCheck
{
	
	public static void main(String[] args) throws IOException
	{
		String yest = TimeUtil.getYesterdayCode();
		
		BluekaiDataMan.setSingQ(TimeUtil.dayBefore(yest));
		
		String datecut = TimeUtil.nDaysBefore(yest, 90);
		Util.pf("Date cutoff is %s\n", datecut);
		
		int num2check = 100000;
		int totalrows = 0;
		int del_user = 0;
		int del_rows = 0;
		
		for(int i = 0; i < num2check; i++)
		{
			BluserPack bpack = BluekaiDataMan.getSingQ().nextPack();
			
			// Util.pf("Reading user %d, has id %s\n", i, bpack.getWtpId());
			
			int rowkill = 0;
			TreeSet<String> segdate = Util.treeset();
			segdate.addAll(bpack.getSegDataMap().values());
			
			for(String onedate : segdate)
			{
				if(onedate.compareTo(datecut) < 0)
					{  rowkill++; }
			}
			
			del_rows += rowkill;
			totalrows += segdate.size();
			
			if(rowkill == segdate.size())
			{
				Util.pf("User has old date, last update is %s, cutoff is %s\n", segdate.last(), datecut);
				del_user++;
			}
		}
		
		Util.pf("Would delete %d users out of %d\n", del_user, num2check);
		Util.pf("Would delete %d rows out of %d\n",  del_rows, totalrows);
		
		
	}
}


