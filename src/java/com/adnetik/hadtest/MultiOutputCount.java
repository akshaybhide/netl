
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


public class MultiOutputCount extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(this);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Specify various job-specific parameters     
		job.setJobName("Domain Counter");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new CountReducer(), a, b, a, b);
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
		
		PathInfo _pInfo = null;

		
		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException
		{
			if(_pInfo == null)
			{
				_pInfo = HadoopUtil.getNfsPathInfoFromReporter(reporter);
			}
			
			lcount++; 
			
			try {
				String logline = value.toString();
				
				BidLogEntry ble = BidLogEntry.getOrNull(_pInfo.pType, _pInfo.pVers, logline);
				if(ble == null)
					{ continue; }
				
				String domain = ble.getField(LogField.domain);		
				
				collector.collect(new Text(domain), new LongWritable(1));
								
			} catch (Exception ex) {
				ercount++;
				//System.out.printf("\n%d err out of %d, line is: %s", lcount, ercount, value.toString());
				//ex.printStackTrace();	
			}
			
			//System.out.printf("\nLCount is %d", lcount++);
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
}
