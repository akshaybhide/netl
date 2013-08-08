
package com.adnetik.data_management;

import java.io.IOException;
import java.io.Console;
import java.util.*;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;

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
import com.adnetik.shared.Util.*;

public class SearchDestroy extends Configured implements Tool
{	
	// These paths are basically valid, they're just too old.
	List<Path> oldList = Util.vector();
	
	// These paths seem to represent an ERROR
	List<Path> errList = Util.vector();
	
	// Paths that we've actually deleted
	List<Path> delList = Util.vector();
	
	FileSystem fsys;
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new SearchDestroy(), args);
		System.exit(exitCode);
	}
		
	public int run(String[] args) throws Exception
	{				
		Map<String, String> settMap = Util.treemap();
		{
			settMap.put("force", "false");	
			Util.putClArgs(args, settMap);
		}
		
		boolean isHard = "true".equals(settMap.get("force"));
		
		
		// FileSystem	
		fsys = FileSystem.get(getConf());

		scanBigLog(LogType.no_bid_all);
		scanBigLog(LogType.bid_all);
		scanBigLog(LogType.bid_pre_filtered);
		
		scanLogType(LogType.imp.toString());
		scanLogType("pixel");
		
		// NEVER delete conversion/click!
		// scanLogType(LogType.conversion);
		// scanLogType(LogType.click);
		
		System.out.printf("\nFound %d old paths, and %d error paths\n\n", oldList.size(), errList.size());

		for(Path p : oldList)
		{
			System.out.printf("\nPath is too old: %s", p);	
			
			if(isHard)
				{ hardDelete(p, false); }
			else
				{ checkDelete(p, false); }
		}
		
		for(Path p : errList)
		{
			if(isHard)
				{ hardDelete(p, true); }
			else
				{ checkDelete(p, true); }
		}
		
		Util.pf("\nCompleted Search and Destroy cleanup\n");
		
		return 0;
	}
	
	void hardDelete(Path p, boolean isErr) throws IOException
	{
		fsys.delete(p, true);
		delList.add(p);
		System.out.printf("\nDeleted %s path %s", (isErr ? "ERROR" : "OLD"), p.toString());		
	}
	
	void checkDelete(Path p, boolean isErr) throws IOException
	{
		System.out.printf("\nDelete %s path %s [y/N]? ", (isErr ? "ERROR" : "OLD"), p.toString());
		
		Console c = System.console();
		String input = c.readLine();
		
		if("y".equals(input))
		{
			fsys.delete(p, true);
			delList.add(p);
			System.out.printf("\nDeleted path %s", p.toString());
		}

	}
	
	void scanBigLog(LogType reltype) throws IOException
	{
		Util.massert(Util.isBig(reltype));
		
		int numSaveDays = Util.numSaveDays(reltype);
		
		Path dataDir = new Path(Util.sprintf("/data/%s/", reltype));
		
		for(FileStatus fstat : fsys.listStatus(dataDir))
		{
			Path sub = fstat.getPath();
			
			try {
				// example path: /data/no_bid
				String[] toks = sub.toString().split("/");
					
				String daycode = toks[toks.length-1];
								
				int daysago = TimeUtil.daysAgo(daycode);
				
				if(daysago > numSaveDays)
					{ oldList.add(sub);}
									
			} catch (Exception ex) {
				
				//System.out.printf("\nPath is invalid: %s", sub.toString());	
				errList.add(sub);
			}
		}					
	}
	
	
	
	void scanLogType(String reltype) throws IOException
	{
		Path dataDir = new Path(Util.sprintf("/data/%s/", reltype));
		
		int numSaveDays = Util.numSaveDays(reltype);
		
		for(FileStatus fstat : fsys.listStatus(dataDir))
		{
			Path sub = fstat.getPath();
			
			try {
				SmartDataFilePath sdp = new SmartDataFilePath(sub.toString());
				
				int daysago = TimeUtil.daysAgo(sdp.dayCode);
				
				if(daysago > numSaveDays)
					{ oldList.add(sub);}
									
			} catch (Exception ex) {
				
				System.out.printf("\nPath is invalid: %s", sub.toString());	
				ex.printStackTrace();
				errList.add(sub);
			}
		}	
	}
	
	
	public static class SmartDataFilePath
	{
		LogType logType;
		ExcName excName;
		String  dayCode;
		
		public SmartDataFilePath(String path) throws Exception
		{
			// EX: hdfs://master/data/imp/yahoo_2011-09-17.lzo.index
			String[] toks = path.split("/");

			int N = toks.length;
			
			Util.massert("data".equals(toks[N-3]));
			
			{
				String lookupTarg = toks[N-2];
				Util.massert(Util.logTypeSaveMap().containsKey(lookupTarg));
			}
			//logType = BidLogEntry.lookupLog(toks[N-2]);
			
			String baseFile = toks[N-1];
			{
				String[] subtoks = baseFile.split("\\.");
			
				// Caller will catch this and deal with it
				if(!"lzo".equals(subtoks[1]))
					{ throw new RuntimeException("Invalid smart file path " + baseFile); }
								
				String[] excday = subtoks[0].split("_");
				
				//excName = Util.excLookup(excday[0]);
				dayCode = excday[1];
				
				//System.out.printf("\nExcname is %s, daycode is %s", excday[0], excday[1]);
			}
		}
	}
}
