
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

public class TopCityQuery extends AbstractMapper.LineFilter
{		
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
		if(ble == null)
			{ return null; }

		String ucity = ble.getField("user_city").trim();
		
		if(ucity.length() == 0)
			{ return null; }

		return new String[] { Util.basicAsciiVersion(ucity), "1" };
		//return new String[] { wtp_id, "1" };
	}
	
	// Subclasses override to modify path behavior
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		String pattern = Util.sprintf("/data/imp/*2012-07*.lzo");
		List<Path> pathlist = HadoopUtil.getGlobPathList(conf, pattern);
		pathset.addAll(pathlist);
		
		//Util.pf("\nPathset is %s, size is %d", pathset, pathset.size());
		//System.exit(1);
	}		
	
	/*
	public static class InvReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		SortedMap<String, String> hitMap = Util.treemap();
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{			
			hitMap.clear();

			String[] wtp_url = key.toString().split(Util.DUMB_SEP);	
			
			while(values.hasNext())
			{
				String[] dt_exc = values.next().toString().split(Util.DUMB_SEP);
				hitMap.put(dt_exc[0], dt_exc[1]);
			}
			
			Calendar frst = Util.longDayCode2Cal(hitMap.firstKey());
			Calendar last = Util.longDayCode2Cal(hitMap.lastKey());
			
			long gap = last.getTimeInMillis() - frst.getTimeInMillis();
			
			long mingap = 30000; // 30 sec
			long maxgap = 60*60*10000; // 1 hr
			
			if(mingap < gap && gap < maxgap)
			{
				String rk = Util.sprintf("%s\t%s", wtp_url[0], wtp_url[1]);
				
				for(String k : hitMap.keySet())
				{
					collector.collect(new Text(rk), new Text(k));
				}
			}
		}		
	}	
	*/
}
