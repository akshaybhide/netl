
package com.adnetik.adhoc;

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
import com.adnetik.shared.Util.*;

public class AdnexusAgeGenderScan extends Configured implements Tool
{
	public static final Integer AGEGEN_VIRT_PIXEL = 67890;
	
	
	public static void main(String[] args) throws Exception
	{
		int excode = ToolRunner.run(new AdnexusAgeGenderScan(), args);
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
			Util.pf("\nUsage: AdnexusAgeGenderScan <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		

		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			//HadoopUtil.alignJobConf(job, new MyMapper(), new LogInterestActivity.MyReducer(), a, a, a, lw);
		}
		
		// File Inputs - either the daily LZO file, if it exists, or the list of GZip files on NFS.
		{
			List<Path> pathlist = Util.vector();
			
			List<Path> nblist = HadoopUtil.getGlobPathList(fSystem, Util.sprintf("/data/no_bid/%s/adnexus/*.log.gz", daycode));
			List<Path> balist = Util.vector();
			//List<Path> balist = HadoopUtil.getGlobPathList(fSystem, Util.sprintf("/data/bid_all/%s/adnexus/*.log.gz", daycode));
			
			Util.pf("\nFound %d no bid paths", nblist.size());
			Util.pf("\nFound %d bid all paths", balist.size());
			
			pathlist.addAll(nblist);
			pathlist.addAll(balist);
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
		Path tempDirPath = HadoopUtil.gimmeTempDir(fSystem);
		Util.pf("\nUsing temp dir %s", tempDirPath);
		FileOutputFormat.setOutputPath(job, tempDirPath);		
		
		System.out.printf("\nCalling AdnexusAgeGenderScan for %s", daycode);
				
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("AdnexusAgeGenderScan %s", daycode));

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
			// Path finalTrgPath = HadoopUtil.getAgeGenInterestPath(daycode);
			Path finalTrgPath = null; 
			Util.massert(false, "Need to rebuild getAgeGenInterestPath(..)");
			HadoopUtil.collapseDirCleanup(fSystem, tempDirPath, finalTrgPath);
		}
		
		HadoopUtil.checkSendFailMail(success, this);		
				
		return 0;
	}
		
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, LogVersion.v13, value.toString());
			if(ble == null)
				{ return; }
			
			String wtp_id = ble.getField(LogField.wtp_user_id).trim();
			String gender = ble.getField(LogField.gender).trim();
			
			// Simple, right? 
			if(gender.length() > 0 && wtp_id.length() > 0)
			{
				output.collect(new Text("" + AGEGEN_VIRT_PIXEL), new Text(wtp_id));
			}
		}
	}
	
	
	/*
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
	*/
}
