
package com.adnetik.hadtest;

import java.io.IOException;
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
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

public class SimpleTestJob extends Configured implements Tool
{
	
	public static final String OUTPUT_PATH = "/home/burfoot/hadtest/SimpleTestJob";
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new SimpleTestJob(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{				
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());		

		HadoopUtil.moveToTrash(fSystem, new Path(OUTPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		FileInputFormat.setInputPaths(job, new Path(TestFileUpload.TEST_FILE_PATH));
		
		{
			Text a = new Text("");	
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new SimpleMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);	
		}
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		checkComputeResult(FileSystem.get(getConf()), OUTPUT_PATH);
		
		return 0;
	}

	static void checkComputeResult(FileSystem fSystem, String outputDir) throws IOException
	{
		List<String> flines = HadoopUtil.readFileLinesE(fSystem, new Path(Util.sprintf("%s/part-00000", outputDir)));
		
		int N = TestFileUpload.MAX_VAL;
		
		for(String s : flines)
		{
			String[] toks = s.split("\t");
			
			int sum = Integer.valueOf(toks[0]);
			int cnt = Integer.valueOf(toks[1]);
			
			int dff = (N-1) - sum;
			dff = (dff < 0 ? -dff : dff);
			
			int trg = N - dff;
			
			//Util.pf("\nFound vals trg=%d, dff=%d, sum=%d, cnt=%d", trg, dff, sum, cnt);
			
			Util.massertEq(trg, cnt);
		}
		
		Util.pf("\nCompute result checked for %d lines\n", flines.size());
	}
	
	
	public static class SimpleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{		
			String[] ijdata = value.toString().split("\t");
			
			//Util.pf("\nLine is %s", value.toString());
			
			try {
				int i = Integer.valueOf(ijdata[0].trim());
				int j = Integer.valueOf(ijdata[1].trim());
				
				String sum = "" + (i+j);		
				
				while(sum.length() < 6)
					{ sum = "0" + sum; }

				output.collect(new Text(sum), new Text("1"));
				
			} catch (Exception ex) {
				
				//Util.pf("\nError on line %s", value.toString());
			}
		}
	}	
}

