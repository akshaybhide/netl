
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

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

public class SortScrub extends Configured implements Tool
{	
	// Delete data that is older than this.
	public static final Integer NUM_SAVE_DAYS = 7;

	
	public static final String BASE_PATH = "/data/analytics/userindex/sortscrub";
	
	public static void main(String[] args) throws Exception
	{
		int subCode = ToolRunner.run(new SortScrub(), args);
		
		System.exit(subCode);
	}
	
	public int run(String[] args) throws Exception
	{	
		HadoopUtil.setLzoOutput(this);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: SortScrub <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		deleteOldData(NUM_SAVE_DAYS, fSystem);
			
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new WtpSorter(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}
		
		// Just want to sort the no_bid data by wtp_user_id
		{
			List<Path> pathlist = Util.vector();
			List<String> patterns = Util.vector();
			
			patterns.add(Util.sprintf("/data/no_bid/%s/*/*.log.gz", daycode));
			patterns.add(Util.sprintf("/data/bid_all/%s/*/*.log.gz", daycode));

			for(String onepatt : patterns)
				{ pathlist.addAll(HadoopUtil.getGlobPathList(fSystem, onepatt)); }
						
			Util.pf("Found %d input files: \n", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
		{
			Path outputPath = new Path(BASE_PATH + "/" + daycode);
			fSystem.delete(outputPath, true);
			Util.pf("\nUsing output dir %s", outputPath);
			FileOutputFormat.setOutputPath(job, outputPath);			
		}
		
		Util.pf("\nCalling SortScrub for %s", daycode);
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("SortScrub %s", daycode));
		job.setNumReduceTasks(HadoopUtil.BIG_JOB_POLITE_NODES); // leave one reducer on each node
		
		// Submit the job, then poll for progress until the job is complete
		RunningJob jobrun = JobClient.runJob(job);		
		HadoopUtil.checkSendFailMail(jobrun, this);

		return 0;
	}
	
	// Cleanup
	void deleteOldData(int numSave, FileSystem fsys) throws IOException
	{
		int listSize = 20;
		
		List<String> daylist = TimeUtil.getDateRange(listSize);		
		
		for(int i = 0; i < listSize - numSave; i++)
		{
			Path targpath = new Path(BASE_PATH + "/" + daylist.get(i)); 	
						
			if(fsys.exists(targpath))
			{
				Util.pf("\nDeleting old data %s", targpath);
				fsys.delete(targpath, true);
			}
		}
		
	}
	
	public static class WtpSorter extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			String line = value.toString();
			
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, LogVersion.v13, line);
			if(ble == null)
				{ return; }
			
			String wtpid = ble.getField("wtp_user_id").trim();
			
			ble.basicScrub();
			
			// Can't do anything with non-set WTP ids
			if(wtpid.length() > 0)
			{ 
				output.collect(new Text(wtpid), new Text(ble.getLogLine()));
			}
		}
	}	
}
