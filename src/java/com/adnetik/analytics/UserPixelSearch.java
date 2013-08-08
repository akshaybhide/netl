	
package com.adnetik.analytics;

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
import com.adnetik.shared.Util.*;
	
public class UserPixelSearch extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new UserPixelSearch(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());	
		
		if(args.length != 2 || !TimeUtil.checkDayCode(args[0]))
		{
			Util.pf("\nUsage UserPixelSearch dayfrom pixellist");	
			return 1;
		}

		String firstday = args[0];
		String pixlist = args[1];

		{
			List<Path> pathlist = HadoopUtil.getPixelLogPaths(getConf(), firstday,  TimeUtil.getYesterdayCode(), true);
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
			//FileInputFormat.setInputPaths(job, new Path[] { pathlist.get(0) } ); 
			
			Util.pf("\nFound %d paths\n", pathlist.size());
		}
		
		String outputPath = HadoopUtil.smartRemovePath(this, "userpixel");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		Util.pf("\nOutput path is %s", outputPath);

		{
			Text a = new Text("");
			HadoopUtil.alignJobConf(job, new UpixelMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}		

		// Specify various job-specific parameters     d
		job.setJobName(Util.sprintf("UserPixelSearch %s", pixlist));
		
		
		{
			String[] pixids = pixlist.trim().split(",");
			for(String pix : pixids)
				{ int pixint = Integer.valueOf(pix); } 
			
			//Util.pf("\nSearching for %d pixels: %s", pixids.length, args[0]);
			job.setStrings("PIX_LIST", pixlist.trim());
		}		
		
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		return 0;
	}
	
	public static class UpixelMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		Set<Integer> targpix = Util.treeset();	
			
		public void configure(JobConf job)
		{
			
			try { 
				String[] pixids = job.get("PIX_LIST").split(",");
				for(String onepix : pixids)
					{ targpix.add(Integer.valueOf(onepix));	}

				Util.pf("\nCONFIGURING, found %d pixels %s", targpix.size(), targpix);

			} catch (Exception ex) {
	
				Util.pf("\nError pixel mapper");
				throw new RuntimeException(ex);
			}
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{			
			PixelLogEntry ple = new PixelLogEntry(value.toString());
			
			int pixelid = ple.getIntField(LogField.pixel_id);
			String wtpid = ple.getField(LogField.wtp_user_id).trim();
			String tstmp = ple.getField(LogField.date_time);
			
			if(targpix.contains(pixelid) && wtpid.length() > 0)
			{
				String resval = Util.sprintf("%s\t%s", wtpid, tstmp);
				output.collect(new Text(""+pixelid), new Text(resval));
			}
		}
	}	
	
}

