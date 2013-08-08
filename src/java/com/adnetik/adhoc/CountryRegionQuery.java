
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
import com.adnetik.shared.*;


public class CountryRegionQuery extends AbstractMapper.LineFilter
{		
	static final int TARG_LINE = 1887945394;
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v18, line);
		if(ble == null)
			{ return null; }
		
		String ctry = ble.getField(LogField.user_country).trim();
		String regn = ble.getField(LogField.user_region).trim();
		String timz = ble.getField(LogField.time_zone).trim();
		
		if(ctry.length() == 0 || timz.length() == 0)
			{ return null; }
		
		if(regn.length() == 0)
			{ regn = "??"; }
		
		
		String ckey = Util.sprintf("%s%s%s%s%s", ctry, Util.DUMB_SEP, regn, Util.DUMB_SEP, timz);
		return new String[] { ckey, "1" }; 
	 
	}
	
	@Override
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{ 
		List<String> range = TimeUtil.getDateRange("2012-08-15", "2012-08-19");
		// String[] range = new String[]  { "2012-08-15" };
		
		for(String daycode : range)
		{
			String patt = Util.sprintf("/data/imp/rtb_%s.lzo", daycode);
		
			//String patt = Util.sprintf("/data/imp/*_%s.lzo", daycode);
			// String patt = Util.sprintf("file:///mnt/adnetik/adnetik-uservervillage/*/userver_log/imp/%s/*.log.gz", daycode);
			List<Path> pathlist = HadoopUtil.getGlobPathList(conf, patt);
			Util.pf("\nFound %d files for pattern %s", pathlist.size(), patt);
			pathset.addAll(HadoopUtil.getGlobPathList(conf, patt));			
		}
		
		// System.exit(1);

		Util.pf("\nFound %d total input paths", pathset.size());
		// System.exit(1);
	}	
	
}
