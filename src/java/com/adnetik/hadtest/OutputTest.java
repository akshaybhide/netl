
package com.adnetik.hadtest;

import java.io.*;
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

//org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,
//org.apache.hadoop.io.compress.BZip2Codec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec


import com.adnetik.shared.*;

public class OutputTest extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new OutputTest(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);
		
		// Create a new JobConf
		/*
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Specify various job-specific parameters     
		job.setJobName("hadoop test job");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			
			HadoopUtil.alignJobConf(job, new MyMapper(), new Util.CountReducer(), a, b, a, b);
		}
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		*/
		return 0;
	}
	
	public static class MyMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, LongWritable>
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException
		{
			String s = value.toString();
			String[] toks = s.split("\t");
			
			String country = toks[2];
			
			collector.collect(new Text(country), new LongWritable(1));
		}
	}
}
