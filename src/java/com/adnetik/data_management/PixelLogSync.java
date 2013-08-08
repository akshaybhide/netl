
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class PixelLogSync extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new PixelLogSync(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());		

		String dayCode = "yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0];
		if(dayCode == null || !TimeUtil.checkDayCode(dayCode))
		{
			Util.pf("\nUsage: PixelLogSync <daycode>");
			return 0;
		}
		
		// Align job
		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new PixLogMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}
		
		System.out.printf("\nCalling PixelLogSync for dayCode=%s", dayCode);
		String destFilePath = HadoopUtil.getHdfsLzoPixelPath(dayCode);

		if(fSystem.exists(new Path(destFilePath)))
		{
			Util.pf("\nFile already exists: %s", destFilePath);
			return 1;
		}
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("PixelLogSync %s", dayCode));
		
		// With alpha=omega, we get one day's worth of data
		List<Path> pathlist = HadoopUtil.getPixelLogPaths(getConf(), Collections.singletonList(dayCode), false);
		Util.pf("\nFound %d NFS pixel log paths", pathlist.size());
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		
		Path outputFilePath = HadoopUtil.gimmeTempDir(fSystem);
		{
			Util.pf("\nUsing temp dir %s\n", outputFilePath);
			FileOutputFormat.setOutputPath(job, outputFilePath);		
		}			
				
		// Submit the job, then poll for progress until the job is complete
		RunningJob runJob = JobClient.runJob(job);		
		
		if(runJob.isSuccessful())
		{
			Util.pf("\nJob successful, reorganizing output...");		
			
			HadoopUtil.collapseDirCleanup(fSystem, outputFilePath, new Path(destFilePath));
			Util.pf("\n");			
		}
		
		return 0;
	}
		
	public static class PixLogMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			// Break log into fields for processing
			String[] fieldList = value.toString().split("\t");
			String jb1 = Util.joinButFirst(fieldList, "\t");
			output.collect(new Text(fieldList[0]), new Text(jb1));
		}
	}
}
