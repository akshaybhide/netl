
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
import com.adnetik.shared.BidLogEntry.LogType;
import com.adnetik.analytics.InterestUserUpdate;

public class BigUserCount extends Configured implements Tool
{	
	public static LogType relType = LogType.imp;
	
	
	public static void main(String[] args) throws Exception
	{
		int mainCode = HadoopUtil.runEnclosingClass(args);		
		System.exit(mainCode);
	}

	public int run(String[] args) throws IOException
	{
		HadoopUtil.setLzoOutput(this);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: BigUserCount <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);
		}
		
		{
			List<Path> pathlist = Util.vector();
			
			for(int i = 0; i < 3; i++)
			{
				String partpath = Util.sprintf("/userindex/sortscrub/%s/part-%s.lzo", daycode, Util.padLeadingZeros(i, 5));
				pathlist.add(new Path(partpath));				
			}
			
			Util.pf("Found %d input files: \n", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}		
		
		// Output path data
		{
			//fSystem.delete(TEMP_NEG_USER_PATH, true);
			Path testpath = new Path("/userindex/usercount/test1");
			HadoopUtil.checkRemovePath(fSystem, testpath);
			//Util.pf("\nUsing temp dir %s", TEMP_NEG_USER_PATH);
			FileOutputFormat.setOutputPath(job, testpath);			
		}
		
		Util.pf("\nCalling BigUserCount for %s", daycode);
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("BigUserCount %s", daycode)); 
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		//Path finalTrgPath = HadoopUtil.getInterestUserPath(daycode, false);
		//HadoopUtil.collapseDirCleanup(fSystem, TEMP_NEG_USER_PATH, finalTrgPath);

		return 0;
	}
	
	
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			String[] toks = value.toString().split("\t");
			output.collect(new Text(toks[0]), HadoopUtil.TEXT_ONE);					
		}
	}
}
