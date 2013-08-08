
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

public class LogInterestActivity extends Configured implements Tool
{
	public static final int	MAX_USER_PER_PIXEL = 20000;
	
	public static void main(String[] args) throws Exception
	{
		int excode = ToolRunner.run(new LogInterestActivity(), args);
		System.exit(excode);
	}
	
	public int run(String[] args) throws Exception
	{			
		HadoopUtil.setLzoOutput(this);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: LogInterestActivity <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);

		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new MyReducer(), a, a, a, lw);
		}
		
		// File Inputs - either the daily LZO file, if it exists, or the list of GZip files on NFS.
		{
			List<Path> pathlist = Util.vector();
			Path lzoPixelPath = new Path(HadoopUtil.getHdfsLzoPixelPath(daycode));
			if(fSystem.exists(lzoPixelPath))
			{
				Util.pf("\nFound LZO pixel path: %s", lzoPixelPath);
				pathlist.add(lzoPixelPath);
				job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);				
			} else {
				pathlist = HadoopUtil.getPixelLogPaths(getConf(), Collections.singletonList(daycode), false);
				Util.pf("\nUsing NFS files, found %d of them", pathlist.size());				
			}
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
		Path tempDirPath = HadoopUtil.gimmeTempDir(fSystem);
		Util.pf("\nUsing temp dir %s", tempDirPath);
		FileOutputFormat.setOutputPath(job, tempDirPath);		
		
		System.out.printf("\nCalling LogInterestActivity for %s", daycode);
				
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("LogInterestActivity %s", daycode));

		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		Path finalTrgPath = HadoopUtil.getInterestUserPath(daycode);
		HadoopUtil.collapseDirCleanup(fSystem, tempDirPath, finalTrgPath);
		
		return 0; 
	}
		
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			try {
				PixelLogEntry ple = new PixelLogEntry(value.toString());
				
				String wtp_id = ple.getField("wtp_user_id").trim();
				String pix_id = ple.getField("pixel_id").trim();
				
				if(wtp_id.length() > 0 && pix_id.length() > 0)
				{
					output.collect(new Text(pix_id), new Text(wtp_id));
					//Util.pf("\nWTP is %s, pix is %s", wtp_id, pix_id);
					//String reskey = Util.sprintf("%s%s%s", wtp_id, Util.DUMB_SEP, pix_id);
					//output.collect(new Text(reskey), HadoopUtil.TEXT_ONE);
				}
			} catch (Exception ex) {
				
				//ex.printStackTrace();
				
			}
		}
	}
	
	
	public static class MyReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, LongWritable>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, LongWritable> collector, Reporter reporter) 
		throws IOException
		{
			String pixid = key.toString();
			Map<String, Integer> idcount = Util.treemap();	
			
			while(values.hasNext())
			{
				String wtp = values.next().toString();
				
				if(idcount.size() < MAX_USER_PER_PIXEL)
				{ 
					Util.incHitMap(idcount, wtp);
				}
			}
			
			for(String wtp_id : idcount.keySet())
			{
				//Util.pf("\nWTP is %s, pix is %s", wtp_id, pix_id);
				String reskey = Util.sprintf("%s%s%s", wtp_id, Util.DUMB_SEP, pixid);
				collector.collect(new Text(reskey), new LongWritable(idcount.get(wtp_id)));		
			}
		}		
	}		
	
	/*
	public static class MyReducer extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
					
			String wtp_pix = 
			
			String wtp_id = ple.getField("wtp_user_id").trim();
			String pix_id = ple.getField("pixel_id").trim();

			if(wtp_id.length() > 0 && pix_id.length() > 0)
			{
				//Util.pf("\nWTP is %s, pix is %s", wtp_id, pix_id);
				String reskey = Util.sprintf("%s%s%s", wtp_id, Util.DUMB_SEP, pix_id);
				output.collect(new Text(reskey), HadoopUtil.TEXT_ONE);
			}
		}
	}	
	*/
}
