
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

public class BidTotalDebug extends AbstractMapper.LineFilter
{		
	public static Set<String> TARG_DOMAINS = Util.treeset();
		
	public String[] filter(String line)
	{
		if(TARG_DOMAINS.isEmpty())
		{
			TARG_DOMAINS.add("foxnews.com");
			TARG_DOMAINS.add("drudgereport.com");
			TARG_DOMAINS.add("autorepairquestions.com");
		}

		BidLogEntry ble = BidLogEntry.getOrNull(LogType.bid_all, LogVersion.v19, line);
		if(ble == null)
			{ return null; }
		
		String domain  = ble.getField(LogField.domain).trim();
		
		if(TARG_DOMAINS.contains(domain))
		{
			int campid = ble.getIntField(LogField.campaign_id);
			double bid = ble.getDblField(LogField.bid);
			
			String resval = Util.sprintf("%d\t%.07f", campid, bid);
			return new String[] { domain, resval };
		}

		return null;
	}
	
	// Subclasses override to modify path behavior
	@Override
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		String opxpatt = "/data/bid_all/2012-12-03/openx/*.log.gz";
		List<Path> pathlist = HadoopUtil.getGlobPathList(conf, opxpatt);
		
		pathset.addAll(pathlist);
	}	
}
