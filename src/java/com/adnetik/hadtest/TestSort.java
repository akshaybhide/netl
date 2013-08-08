
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
import com.adnetik.analytics.*;
import com.adnetik.shared.BidLogEntry.*;
 

public class TestSort extends AbstractMapper.LineFilter
{		
	public String[] filter(String line)
	{ 
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, LogVersion.v13, line);
		if(ble == null)
			{ return null; }
		
		String wtp = ble.getField("wtp_user_id");
		
		if(wtp.length() > 0)
		{
			return new String[] { wtp, line };	
		}
		
		return null;
	}
}
