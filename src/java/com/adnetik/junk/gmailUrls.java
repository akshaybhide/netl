package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.analytics.*;

public class gmailUrls extends AbstractMapper.LineFilter
{		
	public String[] filter(String line)
	{ 
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, line);
		if(ble == null)
			{ return null; } 
		
		String domain = ble.getField("domain");
		
		if(domain.indexOf("gmail.com") > -1 )
		{
			return new String[] { ble.getField("url"), "1" };
		}
		
		return null;
	}
}
