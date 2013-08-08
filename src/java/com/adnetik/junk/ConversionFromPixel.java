
package com.adnetik.data_management;

import java.io.IOException;
import java.io.Console;
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

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;
//import com.adnetik.shared.BidLogEntry.LogType;


public class ConversionFromPixel extends Configured implements Tool
{			
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new ConversionFromPixel(), args);
		System.exit(exitCode);
	}
		
	public int run(String[] args) throws Exception
	{	
		Util.pf("\nRunning conversion from pixel");
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());		

		String daycode = args[0];
		if("yest".equals(daycode))
			{ daycode = TimeUtil.getYesterdayCode(); }	
		
		{
			String pixelpatt = Util.sprintf("file:///mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/%s/*.log.gz", daycode);
			List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), pixelpatt);
			
			Util.pf("\nFound %d pixel paths", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));	
		}
				
		// Align classes
		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}		
		
		job.setJobName(Util.sprintf("Generate Conversion File %s", daycode));
				
		FileSystem fsys = FileSystem.get(getConf());
		Path convOutputPath = new Path(Util.sprintf("/data/analytics/convdata/conv_%s", daycode));
		fsys.delete(convOutputPath, true);
		FileOutputFormat.setOutputPath(job, convOutputPath);	
		
		JobClient.runJob(job);	
		
		// Copy to real destination 
		HadoopUtil.stripDirToFile(FileSystem.get(getConf()), convOutputPath, null);
		return 0;
		
	}
	
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			PixelLogEntry ple = new PixelLogEntry(value.toString());
			int pixid = ple.getIntField("pixel_id");
			String wtpid = ple.getField("wtp_user_id");
			String tstamp = ple.getField("date_time");
			
			if(!"Conversion".equals(ple.getField("pixel_type")))
				{ return; }
			
			if(wtpid.length() == 0)
				{ return; }
			
			String combval = Util.sprintf("%s\t%s", wtpid, tstamp);
			//Util.pf("\npix=%d, combval=%s", pixid, combval);
			output.collect(new Text("" + pixid), new Text(combval));
		}
	}	
}
