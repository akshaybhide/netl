
package com.adnetik.data_management;

import java.io.IOException;
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
import com.adnetik.shared.BidLogEntry.*;

public class LogErrorCheck extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{		
		int exitCode = ToolRunner.run(new LogErrorCheck(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new ErrorMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}		

		String inputFile = "/data/no_bid/2011-12-15/rtb/2011-12-15-23-59-59.EST.no_bid_v13.google-rtb-ireland2_e1a4a.log.gz";
		
					
		String outputFileStr = "/tmp/errorcheck";
		
		HadoopUtil.checkRemovePath(this, outputFileStr);
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("ErrorCheck"));

		FileInputFormat.setInputPaths(job, new Path(inputFile));
		//FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));
		FileOutputFormat.setOutputPath(job, new Path(outputFileStr));		
		
		//job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);

		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);				
		
		return 0;
	}
		
	public static class ErrorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		LogType reltype = LogType.imp;
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			try {
				BidLogEntry ble = new BidLogEntry(LogType.no_bid_all, LogVersion.v13, value.toString());
				Util.pf("\nAd size is %s", ble.getField(LogField.size));
				ble.superStrictCheck();
				
			} catch (BidLogEntry.BidLogFormatException blfe) {
				
				output.collect(new Text(blfe.e.toString()), value);
			}
		}
	}
}
