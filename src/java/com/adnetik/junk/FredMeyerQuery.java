	
package com.adnetik.adhoc;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.analytics.*;
import com.adnetik.shared.Util.*;
	
public class FredMeyerQuery extends AbstractMapper.LineFilter
{
	private static Set<Integer> TARG_SET = Util.treeset();
	
	static {
		TARG_SET.add(3488);
		TARG_SET.add(1702);
		TARG_SET.add(1750);
		TARG_SET.add(2384);
	}
	
	public String[] filter(String line)
	{
		String[] toks = line.trim().split("\t");
		
		if(toks.length < 20)
		{
			// Pixel log entry
			PixelLogEntry ple = PixelLogEntry.getOrNull(line);
			if(ple == null)
				{ return null; }
			
			int pixid = ple.getIntField("pixel_id");
			
			if(TARG_SET.contains(pixid))
			{
				String wtpid = ple.getField("wtp_user_id").trim();
				
				// Does this ever happen?
				if(wtpid.length() == 0)
					{ wtpid = "NotSet"; }				
				
				return new String[] { wtpid, "pixel" + pixid };
			}
			
			
		} else {
			
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
			if(ble == null)
				{ return null; }
			
			int lineid = ble.getIntField("line_item_id");
			 
			
			if(lineid == 1887951190)
			{
				String wtpid = ble.getField("uuid").trim();
				
				if(wtpid.length() == 0)
					{ wtpid = "NotSet"; }				
				
				return new String[] { wtpid, "imp"+lineid };
			}
		}
		
		return null;
	}
	
	public static class FmReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{
			String wtpid = key.toString();
			
			if(wtpid.indexOf("NotSet") > -1)
			{
				int nsimpcount = 0; 
				int nspixcount = 0;
				
				while(values.hasNext())
				{
					// There shouldn't be too many of these...
					String val = values.next().toString();
					
					if(val.startsWith("pixel"))
						{ nspixcount++; }
					
					if(val.startsWith("imp"))
						{ nsimpcount++; }
				}
				
				collector.collect(new Text(wtpid), new Text("NotSetImp=" + nsimpcount));
				collector.collect(new Text(wtpid), new Text("NotSetPix=" + nspixcount));
				return;
			}
			
			
			// Okay, now we are dealing with one specific WTP id
			boolean onlist = false;
			boolean sawimp = false;
			
			// There shouldn't be too many of these...
			while(values.hasNext())
			{
				String val = values.next().toString();
				onlist |= val.startsWith("pixel");
				sawimp |= val.startsWith("imp");
			}			
			
			String valcode = Util.sprintf("ONLIST=%b__SAWIMP=%b", onlist, sawimp);
			collector.collect(new Text(wtpid), new Text(valcode));
		}
	}		
}

