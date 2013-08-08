
package com.adnetik.userindex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

public class NegativeUserQuery extends Configured implements Tool
{
	protected Map<String, String> optArgs = Util.treemap();
	protected Set<Path> pathSet = Util.treeset();
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new NegativeUserQuery(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		Util.putClArgs(args, optArgs);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Set the input paths, either a manifest file or a glob path
		{
			pathSet.addAll(HadoopUtil.pathListFromClArg(getConf(), args[0]));
			//addClPathInfo(pathset, getConf(), optArgs);			
			Util.pf("\nFound %d total input paths", pathSet.size());
			FileInputFormat.setInputPaths(job, pathSet.toArray(new Path[] {} ));		
		}
		
		{
			Text a = new Text("");	
			HadoopUtil.alignJobConf(job, new RandomIdMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}		
		
		String outputPath = HadoopUtil.smartRemovePath(this, "negusers");
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		
		// Specify various job-specific parameters 
		job.setJobName(Util.sprintf("Negative User Query"));
		job.setStrings("SUFF_LIST", "faf");
				
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		
		return 0;
	}
	
	public static class RandomIdMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{		
		List<String> sufflist = Util.vector();
		
		@Override
		public void configure(JobConf job)
		{
			try { 
				String suffstr  = job.get("SUFF_LIST");
				for(String onesuff : suffstr.trim().split(","))
					{ sufflist.add(onesuff.trim()); }
				
				Util.pf("\nFOUND %d SUFFIX  ----------------------", sufflist.size());
				
			} catch (Exception ex) {
				
				Util.pf("\nError loading target id file");
				throw new RuntimeException(ex);
				
			}
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{		
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, value.toString());
			if(ble == null)
				{ return ; }
			
			String wtpid = ble.getField("wtp_user_id").trim();
			String country = ble.getField("user_country").trim();
			
			if(wtpid.length() == 0)
				{ return; }
			
			if(!("US".equals(country)))
				{ return; }
			
			for(String suff : sufflist)
			{
				if(wtpid.endsWith(suff))
				{
					Util.pf("\nFound user %s", wtpid);
					output.collect(new Text(wtpid), new Text(ble.getField("date_time")));	
				}
			}
		}
	}
}

