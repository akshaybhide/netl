
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

public class TimeBigLogScan extends AbstractMapper.LineFilter
{		
	static final int TARG_CAMPAIGN = 610;
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, line);
		if(ble == null)
			{ return null; }
		
		String country = ble.getField("user_country");
		
		if(country.length() == 0)
		{
			country = "NOTSET";	
		}

		return new String[] { country, "1" };
	}
	 
	@Override
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		pathset.addAll(HadoopUtil.getGlobPathList(conf, "/data/bid_all/2012-01-03/*/*.log.gz"));
		pathset.addAll(HadoopUtil.getGlobPathList(conf, "/data/no_bid/2012-01-03/*/*.log.gz"));

	}	
	
}
