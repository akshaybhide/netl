
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


public class CreativeLookup extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new CreativeLookup(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Specify various job-specific parameters     
		job.setJobName("Creative Lookup");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			Text c = new Text("");
			HadoopUtil.alignJobConf(job, new MyMapper(), new SetReducer(), a, b, a, c);
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
		
		public static final LogType REL_TYPE = LogType.conversion;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException
		{
			lcount++;
			
			try {
				String logline = value.toString();
				if(logline.split("\t").length < 4)
					{ return; }
				
				BidLogEntry ble = new BidLogEntry(REL_TYPE, logline);

				String advertiserId = ble.getField("advertiser_id");
				int creativeId = ble.getIntField("creative_id");
								
				String liType = ble.getField("line_item_type");
				String campId = ble.getField("campaign_id");
				String adSize = ble.getField("size");
				
				System.out.printf("\nadsize is %s, campId is %s", adSize, campId);
				
				collector.collect(new Text(advertiserId), new LongWritable(creativeId));
								
			} catch (Exception ex) {
				ercount++;
				//System.out.printf("\n%d err out of %d, line is: %s", lcount, ercount, value.toString());
				//ex.printStackTrace();	
			}
			
			//System.out.printf("\nLCount is %d", lcount++);
		}
	}
	
	public static class SetReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, LongWritable, Text, Text>
	{
		public void reduce(Text key , Iterator<LongWritable> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{
			Set<Long> results = Util.treeset();

			while(values.hasNext())
			{
				long x = values.next().get();	
				results.add(x);
			}
			
			// TODO: this is a hack
			collector.collect(key, new Text(results.toString()));
		}		
	}	
}
