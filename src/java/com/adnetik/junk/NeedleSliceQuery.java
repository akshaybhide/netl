
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

public class NeedleSliceQuery extends Configured implements Tool
{
	protected Map<String, String> optArgs = Util.treemap();
	protected Set<Path> pathset = Util.treeset();
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new NeedleSliceQuery(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		Util.putClArgs(args, optArgs);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		FileSystem fSystem = FileSystem.get(getConf());		
		
		String clicode = args[1];
		
		Set<String> targset = readTargetSet(getConf(), clicode);
		Util.pf("\nFound %d target ids", targset.size());
		
		//job.set("mapred.tasktracker.map.tasks.maximum", "5");
		//job.set("mapred.map.tasks", "5");		
		
		
		// TODO: in principle, this list of input files could be automatically generated.
		// Set the input paths, either a manifest file or a glob path
		{
			String go_back = args[0];
			List<String> date_list = TimeUtil.getDateRange(go_back);
			
			Util.pf("\nUsing %d days of data: %s", date_list.size(), date_list);
			
			for(String onedate : date_list)
			{
				
				// TODO: put NO_BID back in!!!
				//for(String ltype : (new String[] { "no_bid", "bid_all" }))
				for(String ltype : (new String[] { "bid_all" }))
				{
					String nobidpatt = Util.sprintf("/data/%s/%s/*/*.log.gz", ltype, onedate);
					List<Path> nbpath = HadoopUtil.getGlobPathList(fSystem, nobidpatt);
					Util.pf("\nFound %d paths for pattern %s", nbpath.size(), nobidpatt);
					pathset.addAll(nbpath);
				}
			}
			
			// pathset.addAll(HadoopUtil.pathListFromClArg(getConf(), args[0]));
			//addClPathInfo(pathset, getConf(), optArgs);			
			Util.pf("\nFound %d total input paths", pathset.size());
			
			if(pathset.size() == 0)
			{ 
				Util.pf("\nError: no input paths found\n");
				return -1;
			}
			
			FileInputFormat.setInputPaths(job, pathset.toArray(new Path[] {} ));		
		}
		
		{
			Text a = new Text("");	
			HadoopUtil.alignJobConf(job, new LookupMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}		
		
		String outputPath = Util.sprintf("/data/analytics/userindex/needledata/%s/", clicode);
		HadoopUtil.checkRemovePath(this, outputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		
		// Specify various job-specific parameters     d
		job.setJobName(Util.sprintf("Needle Slice Query %s", clicode));
		job.setStrings("CLIENT_CODE", clicode);
				
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		return 0;
	}
	
	static Set<String> readTargetSet(Configuration conf, String clicode) throws IOException
	{
		Set<String> targset = Util.treeset();
		
		String targpath = Util.sprintf("/data/analytics/userindex/targids/%s_ids.txt", clicode);
		for(String idline : HadoopUtil.readFileLinesE(FileSystem.get(conf), targpath))
		{
			targset.add(idline.trim());	 
		}
		
		return targset;
	}
	
	public static class LookupMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{		
		Set<String> targSet;
		
		@Override
		public void configure(JobConf job)
		{
			try { 
				String clicode  = job.get("CLIENT_CODE");
				targSet = readTargetSet(job, clicode);
				Util.pf("\nFOUND %d TARGETS ----------------------", targSet.size());
				
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
			
			if(targSet.contains(wtpid))
			{
				output.collect(new Text(wtpid), value);
			}
		}
	}
}

