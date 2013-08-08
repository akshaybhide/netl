
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


public class JetBlueUniqueCount extends AbstractMapper.LineFilter
{		
	static final int TARG_LINE = 1887945394;
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
		if(ble == null)
			{ return null; }
		
		int lineid = ble.getIntField("line_item_id");
		
		if(lineid == TARG_LINE)
		{
			String uuid = ble.getField("uuid");
			return new String[] { uuid, "1" };
		}

		return null;
	}
	 
	@Override
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{ 
		List<String> range = TimeUtil.getDateRange("2011-07-09", "2011-08-15");
		
		for(String daycode : range)
		{
			String patt = Util.sprintf("file:///mnt/adnetik/adnetik-uservervillage/*/userver_log/imp/%s/*.log.gz", daycode);
			List<Path> pathlist = HadoopUtil.getGlobPathList(conf, patt);
			Util.pf("\nFound %d files for pattern %s", pathlist.size(), patt);
			pathset.addAll(HadoopUtil.getGlobPathList(conf, patt));			
		}
		
		System.exit(1);

	
		
		Util.pf("\nFound %d total input paths", pathset.size());
		//System.exit(1);
	}	
	
}
