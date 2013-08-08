
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

public class CountFileLines extends Configured implements Tool
{
	Set<Path> pathset = Util.treeset();
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{			
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());		
		
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new CountLineMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);
		}
		
		{
			//String patt = "/data/*/rtb_2012-01-28.lzo";
			String patt = "/data/bid_all/2012-03-30/rubicon/*.log.gz";
			
			//String patt = "/data/no_bid/2012-01-31/rtb/*.log.gz";
			List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), patt);	
			
			Util.pf("\nFound %d paths", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));	
			job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);		
		}
		

		String jobCode = "CountFileLines";
		String outputPath = HadoopUtil.smartRemovePath(this, jobCode);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
	
		// Specify various job-specific parameters     d
		job.setJobName(Util.sprintf("Count File Lines %s", jobCode));
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		
		return 0;
	}
	
	public static class CountLineMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		String curPathInfo = null;
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{			
			String onepath =  reporter.getInputSplit().toString();
			
			if(curPathInfo == null)
			{
				curPathInfo = onepath;	
			}
			
			if(onepath.equals(curPathInfo))
			{
				collector.collect(new Text(curPathInfo), HadoopUtil.TEXT_ONE);	
			} else {
				collector.collect(new Text("ERROR"), HadoopUtil.TEXT_ONE);	
			}
		}
	}
}
