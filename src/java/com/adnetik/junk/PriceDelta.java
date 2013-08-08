
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
import com.adnetik.shared.BidLogEntry.LogType;


public class PriceDelta extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new PriceDelta(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), PriceDelta.class);
		
		// Specify various job-specific parameters     
		job.setJobName("Price Delta");
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			
			//HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.CountReducer(), a, b, a, b);
		}
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		return 0;
	}
	
	public static class MyMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, LongWritable>
	{
		int lcount = 0;
		int ercount = 0;
		
		Map<String, Set<String>> cookiePools = null;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException
		{
			lcount++;
			
			try {
				BidLogEntry ble = new BidLogEntry(LogType.imp, value.toString());
				String adex = ble.getField("ad_exchange");
				
				if(!"google".equals(adex))
					{ return; }
				
				double bidPrice = ble.getDblField("bid") * 1000;
				double winPrice = ble.getDblField("winner_price");
				
				long delta = Math.round((bidPrice-winPrice));
				
				//System.out.printf("\nExchange=%s, Bid price is %.03f, win price is %.03f", 
				//		adex, bidPrice, winPrice);
							
				String domain = checkSet(ble.getField("domain"));
				String region = checkSet(ble.getField("user_region"));
				String lineItem = checkSet(ble.getField("line_item_type"));
				
				lineItem = (lineItem.length() == 0 ? "NotSet" : lineItem);
				
				String out_key = Util.sprintf("%s_%s_%s_%d", 
					domain, region, lineItem, delta);
				
				collector.collect(new Text(out_key), new LongWritable(1));
				
			} catch (Exception ex) {
				ercount++;
				//System.out.printf("\n%d err out of %d, line is: %s", lcount, ercount, value.toString());
				//ex.printStackTrace();	
			}
		}
		
		
	}
	
	private static String checkSet(String s)
	{
		return s.length() == 0 ? "NotSet" : s;	
		
	}
}
