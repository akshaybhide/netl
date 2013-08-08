
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

public class JpartnerQuery extends AbstractMapper.LineFilter
{		
	static final int TARG_CAMPAIGN = 610;
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
		if(ble == null)
			{ return null; }
		
		int campid = ble.getIntField("campaign_id");

		if(campid == TARG_CAMPAIGN)
		{
			String uuid = ble.getField("uuid").trim();
			String tstmp = ble.getField("date_time").trim();
			return new String[] { uuid, tstmp };
		}
		
		return null;
	}
	 
	@Override
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		for(String oneday : TimeUtil.getDateRange("2011-10-14", "2011-10-28"))
		{
			String patt = Util.sprintf("/data/imp/*%s.lzo", oneday);
			pathset.addAll(HadoopUtil.getGlobPathList(conf, patt));
		}
	}	
	
}
