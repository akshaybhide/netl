
package com.adnetik.hadtest;

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
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;

public class DumbTestPartition extends AbstractMapper.LineFilter
{		 
	Set<Integer> modset = Util.treeset();
	
	public String[] filter(String line)
	{
		int ftab = line.indexOf("\t");
		String subline = line.substring(ftab);
		
		
		String[] toks = subline.split("\t");
		
		//Util.pf("\nTokens are %s, %s, %s", toks[1], toks[2], toks[3]);
		
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, LogVersion.v13, subline);
		if(ble == null)
			{ return null; }
		
		//String wtpid = ble.getField("wtp_user_id").trim();
		String wtpid = line.substring(0, ftab);
		int hc = wtpid.hashCode();
		hc = (hc < 0 ? -hc : hc);
		int modval = (hc % 24);
		
		if(!modset.contains(modval))
		{
			Util.pf("\nFound new value: %d", modval);
			modset.add(modval);
		}
		
		Util.massert(modset.size() <= 1);
		
		
		return new String[] { ""+modval, "1"};
				
	}
}
