
package com.adnetik.analytics;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

// TODO: merge these things into one file
import com.adnetik.analytics.CountMapWrapper.LineGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.BarGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.HistogramWrapper;


// Scan the day's worth of impression data to create a 
// User Reach database, which is basically just a bunch of <line_item_id, wtp_user_id, timestamp> 
// Triples
public class UserReachScan extends Configured implements Tool
{		
	public static void main(String[] args) throws Exception
	{
		UserReachScan urs = new UserReachScan();
		
		int exitCode = ToolRunner.run(new UserReachScan(), args);
		System.exit(exitCode);
		
		//urs.generateGraphs(args);
	}
	
	// This needs to run AFTER the day's worth of LZO impression files are created.
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// FileSystem	
		FileSystem fsys = FileSystem.get(getConf());
		
		String daycode = args[0];
		
		if("yest".equals(daycode))
			{ daycode = TimeUtil.getYesterdayCode(); }
		
		String globpattern = Util.sprintf("/data/imp/*%s.lzo", daycode);
		//String globpattern = "/data/imp/yahoo_2011-10-14.lzo";
		
		//generateGraphs(fsys);
		//System.exit(1);
		
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, globpattern);
		
		if(pathlist.size() == 0)
		{
			Util.pf("\nError - no HDFS paths found for pattern %s", globpattern);	
			System.exit(1);
		}
		
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));
		//FileInputFormat.setInputPaths(job, new Path[] { pathlist.get(0) });
		
		String outputPath = getOutputPath(daycode);
		HadoopUtil.checkRemovePath(this, outputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		
		{
			Text a = new Text("");	
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new AbstractMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);
		}
		
		{
			job.setStrings("FILTER_CLASS", ReachFilter.class.getName());
			Util.pf("\nclass is %s\n\n", ReachFilter.class.getName());
		}
		
		job.setJobName(Util.sprintf("User Reach Scan %s", daycode));
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		HadoopUtil.stripDirToFile(fsys, new Path(outputPath), null);
		
		return 0;
	}
	
	public static String getOutputPath(String daycode)
	{
		return Util.sprintf("/data/analytics/USER_REACH/data_%s", daycode);	
	}
	
	public static class ReachFilter extends AbstractMapper.LineFilter
	{
		public String[] filter(String line)
		{			
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
			if(ble == null)
				{ return null; }
			
			String wtpId = ble.getField("wtp_user_id").trim();
			String lineItem = ble.getField("line_item_id").trim();
			String campId = ble.getField("campaign_id").trim();
			String lineType = ble.getField("line_item_type").trim();
			
			wtpId = (wtpId.length() == 0 ? "NotSet" : wtpId);
			lineType = (lineType.length() == 0 ? "None" : lineType);
			
			return new String[] { 
				Util.sprintf("%s%s%s%s%s%s%s", campId, Util.DUMB_SEP, lineItem, Util.DUMB_SEP, lineType, Util.DUMB_SEP, wtpId),
				"1"
			};
		}
	}	
	
	

}
