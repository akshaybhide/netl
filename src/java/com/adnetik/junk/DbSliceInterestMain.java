
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;


import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
// import com.adnetik.analytics.InterestUserUpdate;
import com.adnetik.userindex.UserIndexUtil.StagingType;


public class DbSliceInterestMain extends Configured implements Tool
{		
	public enum Counters { PROC_USERS, REDUCER_EXCEPTIONS, LOOKUP_INIT_TIME_SECS, LOOKUP_INIT_COUNT }
	
	public Path tempDirPath;		
	
	SimpleMail logMail;
	
	public static void main(String[] args) throws Exception 
	{
		int mainCode = HadoopUtil.runEnclosingClass(args);
		Util.pf("\n");
	}
	
	public int run(String[] args) throws Exception
	{	
		// This option tells Hadoop to reuse the JVMs
		getConf().setInt("mapred.job.reuse.jvm.num.tasks", 4);	
		
		// getConf().setInt("io.sort.mb", 100);
		// getConf().setInt("mapred.tasktracker.map.tasks.maximum", 1);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: DbSliceInterestMain <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		String logfile = (args.length >= 2 ? args[1] : null);
		logMail = new SimpleMail(Util.sprintf("DbSliceInterestMain %s", daycode));
		
		Map<String, String> optargs = Util.getClArgMap(args);
		Integer maxfile = (optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : Integer.MAX_VALUE);
		
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, true, new DbLookupSlicer(), new HadoopUtil.EmptyReducer(), new UserIndexUtil.SmartPartitioner(),
					a, a, a, a);
		}
		
		// checkStagingInfo(daycode);
		// System.exit(1);
		
		// Inputs are :
		{
			List<Path> pathlist = Util.vector();
			List<String> patterns = Util.vector();
			
			// patterns.add(Util.sprintf("/data/bid_all/%s/casale/*.log.gz", "2012-02-24"));
			patterns.add(Util.sprintf("/data/bid_all/%s/*/*.log.gz", daycode));
			patterns.add(Util.sprintf("/data/no_bid/%s/*/*.log.gz", daycode));
			
			for(String onepatt : patterns)
			{
				List<Path> sublist = HadoopUtil.getGlobPathList(fSystem, onepatt);
				
				for(Path subpath : sublist)
				{
					if(pathlist.size() < maxfile)
						{ pathlist.add(subpath); }
				}
			}
			
			// This allows us to estimate the completion time more accurately.
			Collections.shuffle(pathlist);
			
			logMail.pf("\nFound %d Big Data Log Files", pathlist.size());
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
		// Setup distributed cache 
		// this stuff changed, now one staging file per list request
		/*
		for(StagingType stype : StagingType.values())
		{
			String hdfspath = UserIndexUtil.getStagingInfoPath(stype, daycode);
			
			if((stype == StagingType.click || stype == StagingType.specpcc) && !fSystem.exists(new Path(hdfspath)))
			{ 
				Util.pf("Staging file does not exist for type %s\n", stype);
				continue; 
			}
			
			DistributedCache.addCacheFile(new java.net.URI(hdfspath), job);
		}
		*/
		
		// Listen Code --> index
		Map<String, Integer> listenCodeMap = ListInfoManager.getSing().getListenCodeMap();
		logMail.pf("Found listenCodeMap = %s", listenCodeMap);
		
		Path outputPath = new Path(Util.sprintf("/userindex/catchup/dbslice/%s/", daycode));
		fSystem.delete(outputPath, true);
		logMail.pf("\nUsing directory %s", outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);			
		
		logMail.pf("\nCalling DbSlice-Main for %s", daycode);
		
		// Specify various job-specific parameters     
		job.setJobName(getJobName(daycode));
		
		// Util.pf("Running DbSlice, memory info is : ");
		// Util.showMemoryInfo();
		
		// This job is basically light on the reducers, shouldn't be an issue to use a lot of them.
		job.setNumReduceTasks(listenCodeMap.size()); 
		
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
				
		// NB job can fail and not throw an error.
		if(success)
		{ 
			logMail.pf("\nJob finished, renaming output files");	
			
			for(String listcode : listenCodeMap.keySet())
			{
				int partcode = listenCodeMap.get(listcode);
				Path srcpath = new Path(outputPath, new Path(Util.sprintf("part-%s", Util.padLeadingZeros(partcode, 5))));
				Path dstpath = new Path(outputPath, new Path(listcode + ".slice"));
				fSystem.rename(srcpath, dstpath);
			}
		
		} else {
			logMail.pf("\nJOB FAILED");	
		}
		
		// logMail.logOrSend(logfile);
		
		return 0;
	}
	
	void checkStagingInfo(String daycode) throws IOException
	{
		logMail.pf("\nChecking staging info...");
		FileSystem fsys = FileSystem.get(getConf());

		try { 			
			LookupPack lpack = new LookupPack(daycode);
			lpack.readFromHdfs(fsys);
		}
		catch (IOException ioex) { throw new RuntimeException(ioex); }	
	
		/*		
		for(String listcode : countmap.keySet())
		{ 
			logMail.pf("\nFound %d users for list code %s", countmap.get(listcode), listcode);	
		}
		*/
		
	}
	
	public static String getJobName(String daycode)
	{
		return Util.sprintf("DbSliceInterest-Main %s", daycode);
	}
	
	public static class DbLookupSlicer extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		PathInfo relPath; 
		Path[] localFiles;
		int recCount = 0;
				
		// TODO: need to refactor this; it probably doesn't work as it is 
		LookupPack _lookPack;
		
		@Override
		public void configure(JobConf job)
		{		
			try { 
				localFiles = DistributedCache.getLocalCacheFiles(job); 
			}
			catch (IOException ioex) { 
				ioex.printStackTrace();
			}
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			if(relPath == null)
			{ 
				try { relPath = new PathInfo(reporter.getInputSplit().toString()); }
				catch (Exception ex)  { throw new RuntimeException(ex); }
				
				// This should be equal to total number of input files, or we have a problem
				reporter.incrCounter(HadoopUtil.Counter.PathChecks, 1);  
			}	
			
			/*					
			if(!LookupPack.isReady())
			{
				// FileSystem fsys = FileSystem.get(getConf
				LookupPack.initSing(localFiles); 
				reporter.incrCounter(Counters.LOOKUP_INIT_COUNT, 1);
				// reporter.incrCounter(Counters.LOOKUP_INIT_TIME_SECS, LookupPack.getSing().getLookupTimeSecs());
			}
			*/
			
			String logline = value.toString();
			
			BidLogEntry ble = BidLogEntry.getOrNull(relPath.pType, relPath.pVers, logline);
			if(ble == null)
				{ return; }
			
			String wtpid = ble.getField(LogField.wtp_user_id).trim();
			
			WtpId shortwtp = WtpId.getOrNull(wtpid);
			
			if(shortwtp == null)
				{ return; }
			
			/*
			for(String listcode : LookupPack.getSing().lookupId(shortwtp))
			{ 
				BidLogEntry minimal = ble.transformToVersion(LogType.UIndexMinType, LogVersion.UIndexMinVers2);
				
				// Okay, the followup job is going to partition by pixel id, and then sort.
				String outputkey = Util.sprintf("%s%s%s", listcode, Util.DUMB_SEP, wtpid);
				//String pixstr = padLeadingZeros(pix, 8);
				
				//output.collect(new Text(outputkey), new Text(minimal.getLogLine()));
				output.collect(new Text(outputkey), new Text(minimal.getLogLine()));						
				
				//output.collect(new Text(wtpid), new Text(ble.getLogLine())); 
			}		
			*/
		}
	}		
}
