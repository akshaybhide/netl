
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

import com.adnetik.analytics.CountMapWrapper.LineGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.BarGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.HistogramWrapper;


public class DomainValueGapScan extends Configured implements Tool
{		
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new DomainValueGapScan(), args);
		System.exit(exitCode);
		
		//urs.generateGraphs(args);
	}
	
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
		//String globpattern = args[1];
		//String globpattern = Util.sprintf("/data/imp/yahoo_2011-10-13.lzo", daycode);


		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, globpattern);
		
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));
		
		String outputPath = getOutputPath(daycode);
		HadoopUtil.checkRemovePath(this, outputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		
		{
			Text a = new Text("");	
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new AbstractMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);
		}
		
		{
			job.setStrings("FILTER_CLASS", DvgFilter.class.getName());
			Util.pf("\nclass is %s\n\n", DvgFilter.class.getName());
		}
		
		job.setJobName(Util.sprintf("User Reach Scan %s", daycode));
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		HadoopUtil.stripDirToFile(fsys, new Path(outputPath), null);
		
		return 0;
	}
	
	public static String getOutputPath(String daycode)
	{
		return Util.sprintf("/data/analytics/DomainValueGapScan/data_%s", daycode);	
	}
	
	public static class DvgFilter extends AbstractMapper.LineFilter
	{		
		public String[] filter(String line)
		{		
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
			if(ble == null)
				{ return null; }
			
			Double wprice = ble.getDblField("winner_price");
			Double bid = ble.getDblField("bid");
		
			if(wprice == null || bid == null)
			{
				// TODO: really, this should never happen
				return null;
			}
					
			String adex = ble.getField("ad_exchange").trim();
			
			double convprice = Util.convertPrice(adex, wprice);
			
			String combkey = Util.sprintf("%s%s%.02f",
				adex, Util.DUMB_SEP, (bid-convprice));
			
			return new String[] { combkey, "1" };
		}
	}	
	
	

}
