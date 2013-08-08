
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


public class DomainPriceScan extends Configured implements Tool
{		
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new DomainPriceScan(), args);
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
			job.setStrings("FILTER_CLASS", PriceFilter.class.getName());
			Util.pf("\nclass is %s\n\n", PriceFilter.class.getName());
		}
		
		job.setJobName(Util.sprintf("User Reach Scan %s", daycode));
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		HadoopUtil.stripDirToFile(fsys, new Path(outputPath), null);
		
		return 0;
	}
	
	public static String getOutputPath(String daycode)
	{
		return Util.sprintf("/data/analytics/DOMAIN_PRICE/data_%s", daycode);	
	}
	
	public static class PriceFilter extends AbstractMapper.LineFilter
	{
		static Set<String> targdom = Util.treeset();
		
		static {
			
			targdom.add("myyearbook.com");
			targdom.add("facebook.com");
			targdom.add("facetheme.com");
			targdom.add("okcupid.com");
			targdom.add("mediatakeout.com");
			targdom.add("rincondelvago.com");
			targdom.add("epicfail.com");
			targdom.add("mangafox.net");
			targdom.add("deviantart.com");
			targdom.add("grooveshark.com");
			targdom.add("youtube.com");
		}
		
		
		public String[] filter(String line)
		{		
			BidLogEntry ble;
			try {
				ble = new BidLogEntry(LogType.imp, line);
				ble.strictCheck();
			} catch (BidLogEntry.BidLogFormatException blex) {
				return null;	
			}
			
			Double wprice = ble.getDblField("winner_price");
			if(wprice == null)
				{ return null; }
			
			String adex = ble.getField("ad_exchange");
			String domain = ble.getField("domain").trim();
			
			if(domain.length() == 0)
			{
				String url = ble.getField("url");
				String[] domtop = Util.getDomTopFromUrl(url);
				
				if(domtop == null)
					{ return null; }
				
				domain = Util.sprintf("%s.%s", domtop[0], domtop[1]);
			}
			
			if(!targdom.contains(domain))
				{ return null; }
			
			String combkey = Util.sprintf("%s%s%s%s%.03f",
				adex, Util.DUMB_SEP, domain, Util.DUMB_SEP, wprice);
			
			//Util.pf("\nComb key is %s", combkey);
			
			return new String[] { combkey, "1" };
		}
	}	
	
	

}
