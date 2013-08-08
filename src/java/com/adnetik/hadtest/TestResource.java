
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
import com.adnetik.shared.BidLogEntry.*;

public class TestResource extends Configured implements Tool
{
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new TestResource(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		JobConf job = new JobConf(getConf(), this.getClass());
		
		List<Path> pathlist = Util.vector();
		pathlist.add(new Path("/data/no_bid/2011-12-06/rtb/2011-12-06-23-59-59.EST.no_bid_v13.google-rtb-california6_4310d.log.gz"));
		
		{
			Text a = new Text("");
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new LoadResourceMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);	
		}
		
		
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));
		
		String jobCode = "testresource";
		String outputPath = HadoopUtil.smartRemovePath(this, jobCode);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		return 1;
	}
	
	public static class LoadResourceMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		int mapcount = 0;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{			
			if(mapcount < 100)
			{
				InputStream is = getClass().getResourceAsStream("helloresource.txt");
				for(int x = is.read(); x != -1; x = is.read())
				{
					output.collect(new Text("" + x), HadoopUtil.TEXT_ONE);
				}
				
				is.close();
				
				mapcount++;
			}
		}
	}
	
}
