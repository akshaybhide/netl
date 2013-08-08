
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

public class ConfigErrorTest extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{
		Util.pf("Hello from ConfigErrorTest");
		
		
		// Create a new JobConf
		// Configuration conf = getConf();
		// conf.setBoolean("lzo.text.input.format.ignore.nonlzo", false);		
		getConf().setInt("mapred.tasktracker.reduce.tasks.maximum", 25);
		// int x = conf.getInt("mapred.tasktracker.reduce.tasks.maximum", -1);
		// Util.pf("Max tasks  is %d", x);
		
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Specify various job-specific parameters     
		job.setJobName("Test hadoop parameter change");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.CountReducer(), a, a, a, b);
		}
		
		
		//String myNewPath = "s3n://adnetik-uservervillage/rtb/userver_log/bid_all/2011-08-10/2011-08-11-00-30-02.EDT.bid_all_v12.google-rtb-virginia21.log.gz";
		//FileInputFormat.setInputPaths(job, new Path(myNewPath));	
		{
			List<Path> pathlist = Util.vector();
			pathlist.add(new Path("/tmp/hadtest/TestTwo.txt"));
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));	
		}
		
		String jobCode = "confchange";
		String outputPath = HadoopUtil.smartRemovePath(this, jobCode);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		

		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		return 0;
	}
	
	public static class MyMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text>
	{		
		Set<String> obs = Util.treeset();
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException
		{

			BidLogEntry ble = BidLogEntry.getOrNull(LogType.conversion, LogVersion.v13, value.toString());
			if(ble == null)
				{ return; }
			
			String country = ble.getField("user_country");
			
			if(!obs.contains(country))
			{
				obs.add(country);
				// Util.pf("\n---------------- Observed new country: %s", country);
			}
			
			collector.collect(new Text(country), new Text("1"));
		}
	}	
	
	
}
