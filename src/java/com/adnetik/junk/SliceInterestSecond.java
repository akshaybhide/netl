
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
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.analytics.InterestUserUpdate;
import com.adnetik.userindex.*;

public class SliceInterestSecond extends Configured implements Tool
{	
	public enum Counters { PROC_USERS }
	
	public static void main(String[] args) throws Exception
	{
		int subCode = ToolRunner.run(new SliceInterestSecond(), args);
		System.exit(subCode);
	}

	
	public int run(String[] args) throws Exception
	{		
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: SliceInterestSecond <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);	

		// align
		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new HadoopUtil.EmptyMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}
		
		// File inputs are outputs from SliceMain
		{
			// This is hokey
			String tmppatt = Util.sprintf(SliceInterestActivity.TEMP_SLICE_PATH + "part-*", daycode);
			Util.pf("Temp pattern is %s\n", tmppatt);
			List<Path> pathlist = HadoopUtil.getGlobPathList(fSystem, tmppatt);
			
			if(pathlist.size() == 0)
			{
				Util.pf("\nError: no paths found for pattern %s", tmppatt);	
				return 0;
			}
			
			Util.pf("Found %d input files: \n", pathlist.size());
			
			for(Path p : pathlist)
			{
				Util.pf("\n\t%s", p.toString());
			}
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}			
		
		
		// Take out the check
		Path realOutputPath = new Path("/userindex/sliceactivity/" + daycode);
		fSystem.delete(realOutputPath, true);
		Util.pf("\nUsing directory dir %s", realOutputPath);
		FileOutputFormat.setOutputPath(job, realOutputPath);	
		
		Util.pf("\nCalling SliceInterest-Secondary for %s", daycode);
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("SliceInterest-Secondary %s", daycode));
		job.setNumReduceTasks(HadoopUtil.BIG_JOB_POLITE_NODES);
		
		job.setPartitionerClass(SlicePartitioner.class);
		
		// Submit the job, then poll for progress until the job is complete
		boolean success = false;
		try 
		{
			RunningJob jobrun = JobClient.runJob(job);
			success = jobrun.isSuccessful();
		} catch (Exception ex) {
			success = false;
			ex.printStackTrace();
		}
				
		if(success)
		{
			Path tempSlicePath = new Path(Util.sprintf(SliceInterestActivity.TEMP_SLICE_PATH, daycode));
			Util.pf("Done, deleting temp directory %s", tempSlicePath.toString());
			fSystem.delete(tempSlicePath, true);
		}
		
		HadoopUtil.checkSendFailMail(success, this);
		
		return 0;
	}

	// Key point: all of the info related to a given pixel id should be sent
	// to the same reducer
	public static class SlicePartitioner implements Partitioner<Text, Text>
	{
		public void configure(JobConf jobconf) {
			//super.configure(jobconf);
		}
		
		public int getPartition(Text key, Text value, int numPart)
		{
			String[] pix_wtp = key.toString().split(Util.DUMB_SEP);	
			return UserIndexUtil.partFileFromListId(pix_wtp[0], numPart);
		}
	}
}
