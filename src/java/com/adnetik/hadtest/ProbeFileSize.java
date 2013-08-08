
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.LogType;
import com.adnetik.shared.Util.ExcName;


public class ProbeFileSize extends Configured implements Tool
{
	FileSystem fsys; 
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		fsys = FileSystem.get(getConf());
		
		for(String daycode : TimeUtil.getDateRange(20))
		{
			long daytot = nfsSizeForDate(daycode);
			Util.pf("\nTotal for day %s is %d", daycode, daytot);
		}
		
		return 0;
	}
	
	long nfsSizeForDate(String daycode)
	{
		long total = 0;
		
		for(ExcName exc : ExcName.values())
		{
			Map<String, Long> sizemap = Util.getNfsPathSizeMap(exc, LogType.no_bid_all, daycode);
			
			if(sizemap == null)
				{ continue; }
			
			for(String onep : sizemap.keySet())
				{ total += sizemap.get(onep); } 	
		}
		
		return total; 		
		
	}

	long sizeForDate(String daycode) throws IOException
	{
		String patt = Util.sprintf("/data/no_bid/%s/*/*.log.gz", daycode);
		
		Map<Path, Long> sizemap = HadoopUtil.getGlobPathSizeMap(fsys, patt);
		
		long total = 0;
		
		for(Path onep : sizemap.keySet())
		{
			total += sizemap.get(onep);	
			
		}
		
		
		return total; 
	}
}
