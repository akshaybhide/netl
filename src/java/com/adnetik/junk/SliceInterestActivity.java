
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
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.analytics.InterestUserUpdate;

public class SliceInterestActivity extends Configured implements Tool
{
	// For future reference, this is NOT the way to do things.
	public static String TEMP_SLICE_PATH = "/tmp/sliceinterest/%s/";
	
	public enum Counters { PROC_USERS, REDUCER_EXCEPTIONS }
	
	public Path tempDirPath;		
	
	
	public static void main(String[] args) throws Exception
	{
		int mainCode = ToolRunner.run(new SliceInterestActivity(), args);
		System.exit(mainCode);
	}
	
	public int run(String[] args) throws Exception
	{	

		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: SliceInterestActivity <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new ActivityMapper(), new ActivityReducer(), a, a, a, a);
		}
		
		// Inputs are :
		// 1) ALL interest-user files
		// 2) ONE DAY'S worth of Big Data
		{
			List<Path> pathlist = Util.vector();
			List<String> patterns = Util.vector();
			
			// TODO, possibly want to limit this to last 30 days... ? maybe it doesn't matter
			// Okay, this is going to automatically include agegen and any other types
			// of new interest files we put in.
			patterns.add("/userindex/interest/*.lzo");

			for(String onepatt : patterns)
			{
				pathlist.addAll(HadoopUtil.getGlobPathList(fSystem, onepatt));
			}
			
			// BIGTODO: change this to number of reducers
			for(int i = 0; i < 24; i++)
			{
				Path p = new Path(Util.sprintf("/userindex/sortscrub/%s/part-%s.lzo", daycode, Util.padLeadingZeros(i, 5)));
				
				if(fSystem.exists(p))
					{ pathlist.add(p); }
				else
					{ Util.pf("\nWARNING Path does not exist %s", p); }
			}
			
			Util.pf("Found %d input files: \n", pathlist.size());
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
		Path tempSlicePath = new Path(Util.sprintf(TEMP_SLICE_PATH, daycode));
		fSystem.delete(tempSlicePath, true);
		Util.pf("\nUsing temp dir %s", tempSlicePath);
		FileOutputFormat.setOutputPath(job, tempSlicePath);			
		
		Util.pf("\nCalling SliceInterest-Main for %s", daycode);
		
		// Specify various job-specific parameters     
		job.setJobName(getJobName(daycode));
		job.setNumReduceTasks(HadoopUtil.BIG_JOB_POLITE_NODES); // defaults to 1
		
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
						
		HadoopUtil.checkSendFailMail(success, this);		
		
		return 0;
	}
	
	public static String getJobName(String daycode)
	{
		return Util.sprintf("SliceInterest-Main %s", daycode);
	}
	
	
	public static class ActivityMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			String line = value.toString();
			
			if(line.split("\t").length > 10)
			{
				// Okay, here the inputs are now <id, logline> tuples
				int tabind = line.indexOf("\t");
				String logline = line.substring(tabind+1);
				
				// The input is a Bid Log entry
				// emit <id, line>
				// TODO: does no_bid and bid_all have the same structure????
				BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, LogVersion.v13, logline);
				if(ble == null)
					{ return; }
				
				String wtpid = ble.getField("wtp_user_id").trim();
				
				ble.basicScrub();

				// Can't do anything with non-set WTP ids
				if(wtpid.length() > 0)
				{ 
					output.collect(new Text(wtpid), new Text(ble.getLogLine()));
				}
				
				
			} else {
				
				// User activity line
				// TODO: use regexps to do this
				try {
					String[] reskey_count = line.split("\t");
					String[] wtpid_pixel = reskey_count[0].split(Util.DUMB_SEP);
					output.collect(new Text(wtpid_pixel[0]), new Text(wtpid_pixel[1]));
				} catch (Exception ex ) {
					
					// This is probably a malformed BidLogEntry, that gets interpreted as 
					// a Interest Activity line.
				}
			}
		}
	}
	
	public static class ActivityReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			try {
				Set<Integer> foundpix = Util.treeset();
				List<String> foundlines = Util.vector();			
				
				while(values.hasNext())
				{
					String line = values.next().toString().trim();
					
					if(line.split("\t").length > 4)
					{
						// This is a big log entry line
						if(foundlines.size() < 1000)
							{ foundlines.add(line); }
					} else {
						
						// Aha! Interest user for given pixel id
						Integer pixid = Integer.valueOf(line);
						foundpix.add(pixid);
					}
				}
				
				// Emit <pixid, line> pairs 
				// Data can be replicated!! If user is on list corresponding to multiple pixel ids.
				for(Integer pix : foundpix)
				{
					for(String callout : foundlines)
					{
						// pixel ids are like 2450, so 8 digits should be enough for a while.
						
						// Okay, the followup job is going to partition by pixel id, and then sort.
						String outputkey = Util.sprintf("%s%s%s", padLeadingZeros(pix, 8), Util.DUMB_SEP, key.toString());
						//String pixstr = padLeadingZeros(pix, 8);
						collector.collect(new Text(outputkey), new Text(callout));					
					}
				}
				
				reporter.incrCounter(Counters.PROC_USERS, 1);
			} catch (Exception ex) {
				
				reporter.incrCounter(Counters.REDUCER_EXCEPTIONS, 1);
			}
		}		
		
		// TODO: put this in Util
		private String padLeadingZeros(int val, int numDigits)
		{
			String s = "" + val;
			
			while(s.length() < numDigits)
				{ s = "0" + s; }
			
			return s;
		}
	}		
}
