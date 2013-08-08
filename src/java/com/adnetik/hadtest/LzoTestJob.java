
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
 
// Test that we can read LZO files in Hadoop
public class LzoTestJob extends Configured implements Tool
{
	public static final String TEST_LZO_PATH = "/home/burfoot/hadtest/fakedata.txt.lzo";
	public static final String TEST_OUTPUT_DIR = "/home/burfoot/hadtest/LzoTestJob";
	
	public static void main(String[] args) throws Exception
	{
		//System.out.printf("\nFile %s--", "file:///");
		int exitCode = ToolRunner.run(new LzoTestJob(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{				
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());		

		HadoopUtil.checkRemovePath(this, new Path(TEST_OUTPUT_DIR));
		FileOutputFormat.setOutputPath(job, new Path(TEST_OUTPUT_DIR));
		FileInputFormat.setInputPaths(job, new Path(TEST_LZO_PATH));
		
		{
			Text a = new Text("");	
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new SimpleTestJob.SimpleMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);	
		}
		
		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		JobClient.runJob(job);	

		SimpleTestJob.checkComputeResult(FileSystem.get(getConf()), TEST_OUTPUT_DIR);
		
		return 0;
	}
}
