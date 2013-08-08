
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
import com.adnetik.data_management.*;
import com.adnetik.userindex.*;
import com.adnetik.shared.BidLogEntry.LogType;


public class MapperMemCheck extends Configured implements Tool
{
	public enum Counter { MemInfo };
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Specify various job-specific parameters     
		job.setJobName("Check Mapper memory");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.CountReducer(), a, a, a, b);
		}
		
		String inputpath = "/data/no_bid/2012-04-10/rtb/2012-04-10-23-59-59.EDT.no_bid_v14.google-rtb-virginia37_364b3.log.gz";
		
		String outputPath = HadoopUtil.smartRemovePath(this, "CheckMem");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		FileInputFormat.setInputPaths(job, inputpath);	
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		return 0;
	}
	
	public static class MyMapper extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text>
	{	
		public static final LogType REL_TYPE = LogType.imp;
		
		static Map<WtpId, Set<String>> PIXMAP;
		
		boolean memreport = false;
		
		String dayCode = "2012-04-05";
		
		@Override
		public void configure(JobConf job)
		{			
			if(PIXMAP == null)
			{
				PIXMAP = Util.treemap();
				Map<String, Integer> countmap = Util.treemap();
				
				try {
					FileSystem fsys = FileSystem.get(job);
					
					for(UserIndexUtil.StagingType onetype : UserIndexUtil.StagingType.values())
					{
						String infopath = UserIndexUtil.getStagingInfoPath(onetype, dayCode);
						Scanner infoscan = HadoopUtil.hdfsScanner(fsys, infopath);
						//int excount = DbSliceInterestMain.populateLookupMap(PIXMAP, countmap, infoscan);
						infoscan.close();
						
						System.gc();
						// TODO: this is fucked up
						// logMail.pf("\nFound %d valid IDs, %d errors for type: %s", PIX_LOOKUP_MAP.size(), excount, onetype);
					}
					
				} catch (IOException ioex) {
					throw new RuntimeException(ioex);	
				}	
			}
		}		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException
		{
			String logline = value.toString();

			Runtime rt = Runtime.getRuntime();
			long maxmem = rt.maxMemory();
			String maxkey = Util.sprintf("maxmem=%d", maxmem);
			
			if(!memreport)
			{	
				reporter.incrCounter(Counter.MemInfo, maxmem);
				memreport = true;
			}
			
			collector.collect(new Text(maxkey), HadoopUtil.TEXT_ONE);
		}
	}
}
