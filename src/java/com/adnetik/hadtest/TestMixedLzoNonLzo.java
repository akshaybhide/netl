
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

// This test confirms that it is possible to run Hadoop jobs using mixed LZO and non-LZO inputs,
// using the "lzo.text.input.format.ignore.nonlzo" option set to FALSE.
// test1: use two LZO files
// test2: use one LZO file, unzipped version of second file
// test3: use one LZO and one TXT, but don't turn on the special flag
// Test results are that test1+test2 are identical, while test3 is different.
public class TestMixedLzoNonLzo extends Configured implements Tool
{
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new TestMixedLzoNonLzo(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);		
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Specify various job-specific parameters     
		job.setJobName("Test multiple LZO/non-LZO inputs");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.CountReducer(), a, a, a, b);
		}
		
		
		//String myNewPath = "s3n://adnetik-uservervillage/rtb/userver_log/bid_all/2011-08-10/2011-08-11-00-30-02.EDT.bid_all_v12.google-rtb-virginia21.log.gz";
		//FileInputFormat.setInputPaths(job, new Path(myNewPath));	
		{
			List<Path> pathlist = Util.vector();
			pathlist.add(new Path("/tmp/hadtest/TestOne.lzo"));
			//pathlist.add(new Path("/tmp/hadtest/TestTwo.lzo"));
			pathlist.add(new Path("/tmp/hadtest/TestTwo.txt"));
			
			job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
			
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));	
		}
		
		String jobCode = "mixed3";
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
				Util.pf("\n---------------- Observed new country: %s", country);
			}
			
			collector.collect(new Text(country), new Text("1"));
		}
	}
}
