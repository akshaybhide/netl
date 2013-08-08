
package com.adnetik.adhoc;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.analytics.*;
import com.adnetik.shared.Util.*;	



public class UrlVolume extends AbstractMapper.LineFilter
{
	public static String DUMB_SEP = "____";
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, line);
		
		if(ble == null)
			{ return null; }
		
		String url = ble.getField("url").trim();
		String main_vert = ble.getField("google_main_vertical").trim();
		String exc_id = ble.getField("exchange_user_id").trim();
		
		String country = ble.getField("user_country").trim();
		
		if(!country.equals("US"))
			{ return null; }
		
		if(url.length() == 0 || exc_id.length() == 0)
			{return null; }
		
		String combKey = Util.sprintf("%s%s%s", url, DUMB_SEP, exc_id);
				
		return new String[] { combKey, main_vert };
	}
	
	public static class DiffKeyReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		String cururl = null;
		String curid = null;
		
		int uniqueCount = 0;
		
		Set<String> vertset = Util.treeset(); // this can't be TOO large
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{
			String[] urlid = key.toString().split(DUMB_SEP);
			//Util.pf("\nFound url=%s and curid=%s", cururl, curid);
			
			if(!urlid[0].equals(cururl))
			{
				// Flush the data associated with the previous url.				
				if(cururl != null)
				{
					//Util.pf("\nFound url=%s and curid=%s", cururl, curid);
					collector.collect(new Text(cururl), new Text(getResultString()));
				}
				
				vertset.clear();
				uniqueCount = 1;
				
				cururl = urlid[0];
				curid = urlid[1];
			}
			
			if(!urlid[1].equals(curid))
			{
				uniqueCount++;	
				curid = urlid[1];
			}
			
			// There shouldn't be too many of these...
			while(values.hasNext())
			{
				vertset.add(values.next().toString());
			}
		}

		private String getResultString()
		{
			StringBuffer sb = new StringBuffer();
			sb.append(uniqueCount);
			sb.append("\t");
			
			String[] verts = vertset.toArray(new String[] {} );
			sb.append(Util.joinButFirst(verts, ","));
			
			return sb.toString();
		}
	}	
}

