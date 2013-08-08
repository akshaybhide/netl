
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


public class JobAlignTest extends Configured implements Tool
{
	
	public static void main(String[] args) throws Exception
	{
		// If we call TestMixedLzoNonLzo, we should get a Job Alignment Error on HadoopUtil.alignJobConf
		int exitCode = ToolRunner.run(new TestMixedLzoNonLzo(), args);
		//int exitCode = ToolRunner.run(new JobAlignTest(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		//getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);		
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Specify various job-specific parameters     
		job.setJobName("Job Align  Test");
		
		{
			Text a = new Text("");
			LongWritable b = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new HadoopUtil.EmptyMapper(), new HadoopUtil.CountReducer(), a, a, a, b);
		}
		
		Util.pf("\nCongratulations, you are calling the right class\n");		
		return 0;
	}
	
	public static class FakeJob extends Configured implements Tool
	{
		public int run(String[] args) throws Exception
		{
			//getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);		
			
			// Create a new JobConf
			JobConf job = new JobConf(getConf(), this.getClass());
			
			// Specify various job-specific parameters     
			job.setJobName("Fake Job  Test");
			
			{
				Text a = new Text("");
				LongWritable b = new LongWritable(0);
				HadoopUtil.alignJobConf(job, new HadoopUtil.EmptyMapper(), new HadoopUtil.CountReducer(), a, a, a, b);
			}
			
			
			throw new RuntimeException("Should not reach here - should error on alignJobConf");
		}		
		
		
	}
}
