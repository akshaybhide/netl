
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

public class DbhUnique extends AbstractMapper.LineFilter
{		
	public static final int TARG_CAMP_ID = 1026;
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
		if(ble == null)
			{ return null; }
		
		String adex = ble.getField("ad_exchange");
		String wtpid = ble.getField("wtp_user_id");
		int campid = ble.getIntField("campaign_id");
		
		if(campid != TARG_CAMP_ID)
			{ return null; }
		
		if(wtpid.trim().length() == 0)
			{ return null; }
		
		
		String isdbh = adex.equals("dbh") ? "dbh" : "nondbh";
		
		return new String[] { wtpid, isdbh };
	}

	public List<Path> getExtraPaths(Configuration conf) throws IOException
	{
		FileSystem fsys = FileSystem.get(conf);
		List<Path> pathlist = Util.vector();
		
		//Dates: 11/21/11 12/16/11
		List<String> daterange = TimeUtil.getDateRange("2011-12-07", "2011-12-16");
		
		for(String daycode : daterange)
		{
			String pathpatt = Util.sprintf("/data/imp/*_%s.lzo", daycode);
			List<Path> newlist = HadoopUtil.getGlobPathList(fsys, pathpatt);
			Util.pf("\nFound %d paths for pattern %s", newlist.size(), pathpatt);
			pathlist.addAll(newlist);
		}
		
		return pathlist;
	}
	
}
