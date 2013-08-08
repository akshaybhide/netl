
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
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.shared.*;
import com.adnetik.analytics.*;
import com.adnetik.userindex.*;

public class TopDomainQuery extends AbstractMapper.LineFilter
{		
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.bid_all, LogVersion.v19, line);
		if(ble == null)
			{ return null; }
		
		String domain = ble.getField(LogField.domain).trim();
		String country = ble.getField(LogField.user_country).trim();
		
		if(!UserIndexUtil.COUNTRY_CODES.contains(country))
			{ return null; }
	
		
		String ckey = Util.sprintf("%s%s%s", country, Util.DUMB_SEP, domain);
		return new String[] { ckey, "1" }; 
	 
	}
	
	@Override
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{ 
		for(String daycode : TimeUtil.getDateRange("2012-10-09", "2012-10-14"))
		{
			String simppatt = Util.sprintf("/data/bid_all/%s/*/*.log.gz", daycode);
			List<Path> pathlist = HadoopUtil.getGlobPathList(conf, simppatt);
			pathset.addAll(pathlist);
		}
		

	}	
	
}
