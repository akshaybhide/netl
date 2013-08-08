
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

public class CountryCount extends AbstractMapper.LineFilter
{		
	public static Set<String> TARG_DOMAINS = Util.treeset();
		
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v20, line);
		 
		String country = ble.getField(LogField.user_country);
		
		if(country.trim().length() > 0)
		{
			return new String[] { country, "1" };	
		}
		
		return null;
	}
	 
	// Subclasses override to modify path behavior
	@Override
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		String curday = "2013-01-20";
		
		for(int n = 0; n < 5; n++)
		{
			String daypatt = Util.sprintf("/data/imp/*%s*.lzo", curday);
			List<Path> pathlist = HadoopUtil.getGlobPathList(conf, daypatt);
			Util.pf("Found %d paths for pattern %s\n", pathlist.size(), daypatt);
			pathset.addAll(pathlist);
			curday = TimeUtil.dayAfter(curday);
		}
			
		
		// String opxpatt = "/data/bid_all/2012-12-03/openx/*.log.gz";
		// List<Path> pathlist = HadoopUtil.getGlobPathList(conf, opxpatt);
		
		// pathset.add(mypath);
	}	
}
