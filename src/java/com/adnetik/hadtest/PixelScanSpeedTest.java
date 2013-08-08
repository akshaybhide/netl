
package com.adnetik.hadtest;

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
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;


// Question: how long does it take to scan 60 days of Pixel Log Data?
public class PixelScanSpeedTest extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int excode = HadoopUtil.runEnclosingClass(args);
		System.exit(excode);
	}
	
	public int run(String[] args) throws Exception
	{			
		HadoopUtil.setLzoOutput(this);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
				
		//String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);

		{
			List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), "/data/pixel/pix_*.lzo");
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
		}		
				
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);
		}
		
		String outputPath = HadoopUtil.smartRemovePath(this, "PixelScanSpeed");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		
				
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("PixelScanSpeedTest"));

		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		return 0; 
	}
		
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			try {
				PixelLogEntry ple = new PixelLogEntry(value.toString());
				
				String pix_id = ple.getField("pixel_id").trim();
				
				if(pix_id.length() > 0)
				{
					output.collect(new Text(pix_id), HadoopUtil.TEXT_ONE);
				}
			} catch (Exception ex) {
				
				//ex.printStackTrace();
				
			}
		}
	}
}
