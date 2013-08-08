
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

public class JohnRandQuery extends AbstractMapper.LineFilter
{		
	static Set<Integer> TARG_LINE_SET = Util.treeset();
	static final int TARG_PIXEL_ID = 3161;
	
	static {
		TARG_LINE_SET.add(1887952403);
		TARG_LINE_SET.add(1887952201);
		TARG_LINE_SET.add(1887952200);
		TARG_LINE_SET.add(1887952199);
		TARG_LINE_SET.add(1887952198);
		TARG_LINE_SET.add(1887952197);		
	}
	
	public String[] filter(String line)
	{ 
		if(line.split("\t").length > 30)
		{
			// Impression Log
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
			if(ble == null)
				{ return null; }			
			
			int line_id = ble.getIntField("line_item_id");
			
			if(TARG_LINE_SET.contains(line_id))
			{
				String wtp = ble.getField("wtp_user_id").trim();
				if(wtp.length() == 0)
					{ wtp = "NotSet"; }
				
				String val = Util.sprintf("imp\t%d", line_id);
				
				return new String[] { wtp, val };
			}
			
		} else {
			// pixel log line
			PixelLogEntry ple = PixelLogEntry.getOrNull(line);
			if(ple == null)
				{ return null;} 
			
			int pix_id = ple.getIntField("pixel_id");
			
			if(pix_id == TARG_PIXEL_ID)
			{
				String wtp = ple.getField("wtp_user_id").trim();
				if(wtp.length() == 0)
					{ wtp = "NotSet"; }			
				
				String val = Util.sprintf("pix\t%d", pix_id);

				return new String[] { wtp, val };
			}
		}
		
		return null;
	}
	
	/*
	@Override
	public List<Path> getExtraPaths(Configuration conf) throws IOException
	{
		List<Path> implist = Util.vector();
		List<String> daterange = TimeUtil.getDateRange("2011-11-15", "2011-12-01");
		
		for(String daycode : daterange)
		{
			String daypatt = Util.sprintf("/data/imp/*%s.lzo", daycode);
			List<Path> plist = HadoopUtil.getGlobPathList(conf, daypatt);
			Util.pf("\nFound %d extra paths for pattern %s", plist.size(), daypatt);
			implist.addAll(plist);
		}
		
		return implist;
	}
	*/
}
