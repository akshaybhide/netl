
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

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
 
// Test that we can read LZO files in Hadoop
public class TestLzoWrite extends Configured implements Tool
{
	public static final String FAKE_DATA_PATH = "/mnt/burfoot/hadtest/fakedata.txt";
	public static final String FAKE_DATA_LZO_PATH = FAKE_DATA_PATH + ".lzo";

	public static final String TEST_OUTPUT_DIR = "/mnt/burfoot/hadtest/TestLzoWrite";
	
	public static void main(String[] args) throws Exception
	{
		//System.out.printf("\nFile %s--", "file:///");
		int exitCode = ToolRunner.run(new TestLzoWrite(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{				
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);		
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());		

		HadoopUtil.checkRemovePath(this, new Path(TEST_OUTPUT_DIR));
		FileOutputFormat.setOutputPath(job, new Path(TEST_OUTPUT_DIR));
		FileInputFormat.setInputPaths(job, new Path(FAKE_DATA_PATH));
		
		{
			Text a = new Text("");	
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new NoopMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);	
		}
		
		JobClient.runJob(job);	

		//SimpleTestJob.checkComputeResult(FileSystem.get(getConf()), TEST_OUTPUT_DIR);
		
		{
			String lzoFile = Util.sprintf("%s/part-00000.lzo", TEST_OUTPUT_DIR);
			fSystem.rename(new Path(lzoFile), new Path(FAKE_DATA_LZO_PATH));
			fSystem.delete(new Path(TEST_OUTPUT_DIR), true);
			Util.pf("\nFinished job and copied output file to path %s\n", FAKE_DATA_LZO_PATH);
		}
		
		return 0;
	}

	public static class NoopMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{		
			String[] ijdata = value.toString().split("\t");
			
			if(ijdata.length < 2)
				{ return; }
			
			output.collect(new Text(ijdata[0]), new Text(ijdata[1]));
		}
	}		
	
}
