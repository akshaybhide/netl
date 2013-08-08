
package com.adnetik.analytics;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;

public class ConversionAnalysis extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new ConversionAnalysis(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), ConversionAnalysis.class);
		
		// Specify various job-specific parameters     
		job.setJobName("Conversion Analysis");
		
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
		
		Map<String, Set<String>> cookiePools = null;
		
		private void initCookiePools()
		{
			// read in cookie pools
			cookiePools = Util.treemap();

			String[] localFiles = (new File(".")).list();			
			for(String locFile : localFiles)
			{
				if(locFile.endsWith(".id.pool"))
				{
					System.out.printf("\nFound local cookie pool file %s", locFile);
					String poolName = locFile.split(".")[0];
					
					Set<String> ids = readPoolIds(locFile);
					cookiePools.put(poolName, ids);
				}
			}
		}
		
		private Set<String> readPoolIds(String fileName)
		{
			Set<String> idset = Util.treeset();
			int lcount = 0;
			
			try {
				Scanner sc = new Scanner(new File(fileName));	
				
				while(sc.hasNextLine())
				{
					String oneid = sc.nextLine().trim();
					idset.add(oneid);
					lcount++;
				}
				
				sc.close();

			} catch (IOException ioex) {				
				ioex.printStackTrace(System.err);
			}
			
			System.err.printf("\nRead %d cookie ids in %d lines from file %s",
							idset.size(), lcount, fileName);
			
			return idset;
		}
		
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
}
