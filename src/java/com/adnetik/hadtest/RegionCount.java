
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


public class RegionCount extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new RegionCount(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), RegionCount.class);
		
		// Specify various job-specific parameters     
		job.setJobName("countjob");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			
			alignJobConf(job, new MyMapper(), new CountReducer(), a, b, a, b);
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
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException
		{
			lcount++;
			
			try {
				String s = value.toString();
				String[] toks = s.split("\t");
				
				String country = toks[83];
				
				collector.collect(new Text(country), new LongWritable(1));
								
			} catch (Exception ex) {
				ercount++;
				//System.out.printf("\n%d err out of %d, line is: %s", lcount, ercount, value.toString());
				//ex.printStackTrace();	
			}
			
			//System.out.printf("\nLCount is %d", lcount++);
		}
	}
	
	public static class MyReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key , Iterator<LongWritable> values, OutputCollector<Text,LongWritable> collector, Reporter reporter) 
		throws IOException
		{
			long maxVal = -1;
					
			while(values.hasNext())
			{
				long x = values.next().get();	
				maxVal = (x > maxVal ? x : maxVal);
			}
			
			//collector.collect(key, new LongWritable(maxVal));
			collector.collect(key, new LongWritable(maxVal));
		}		
	}
	
	public static class CountReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, LongWritable, Text, LongWritable>
	{
		public void reduce(Text key , Iterator<LongWritable> values, OutputCollector<Text,LongWritable> collector, Reporter reporter) 
		throws IOException
		{
			long count = 0;
			
			
			while(values.hasNext())
			{
				long x = values.next().get();	
				count += x;
			}
			
			//collector.collect(key, new LongWritable(maxVal));
			collector.collect(key, new LongWritable(count));
		}		
	}	
	
	// This provide type safety. Once you get this to compile,
	// you're guaranteed not to get errors at runtime from mismatch between job types.
	// TODO: figure out how to use Reflection or whatever so that we don't
	// need to include all the example objects in the signature.
	static <AKEY, AVAL, BKEY, BVAL, CKEY, CVAL> void  alignJobConf(JobConf job, 
			Mapper<AKEY, AVAL, BKEY, BVAL> theMap,
			Reducer<BKEY, BVAL, CKEY, CVAL> theRed,
			BKEY bkey, BVAL bval, CKEY ckey, CVAL cval)
	{
		// Set the outputs for the Map
		job.setMapOutputKeyClass(bkey.getClass());
		job.setMapOutputValueClass(bval.getClass());		
		
		// Set the outputs for the Map
		job.setOutputKeyClass(ckey.getClass());
		job.setOutputValueClass(cval.getClass());		

		job.setMapperClass(theMap.getClass());		
		job.setReducerClass(theRed.getClass());		
	}	
}
