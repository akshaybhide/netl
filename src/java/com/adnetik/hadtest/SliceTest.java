
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.net.*;

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
import org.apache.hadoop.filecache.*;


import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;
// import com.adnetik.analytics.InterestUserUpdate;
import com.adnetik.userindex.UserIndexUtil.StagingType;
import com.adnetik.data_management.*;


public class SliceTest extends Configured implements Tool
{
	private static final int VALSTART = 2;  // val offset in acct
	private static final int ACCTSIZE = 3;  // total #fields in acct
	private static final int RECSIZE = (ACCTSIZE + 1) * 4;  // acct bytes per record	
	
	
	// For future reference, this is NOT the way to do things.
	public static String TEMP_SLICE_PATH = "/tmp/sliceinterest/%s/";
		
	public enum Counters { PROC_USERS, REDUCER_EXCEPTIONS, LOOKUP_INIT_TIME_SECS, LOOKUP_INIT_COUNT }
	
	public Path tempDirPath;		
		
	public static void main(String[] args) throws Exception
	{
		int mainCode = HadoopUtil.runEnclosingClass(args);
		Util.pf("\n");
	}
	
	public int run(String[] args) throws Exception
	{	
		// This option tells Hadoop to reuse the JVMs
		getConf().setInt("io.sort.mb", 100);		
		//getConf().setInt("mapred.tasktracker.map.tasks.maximum", 1)


		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		final float spillper = job.getFloat("io.sort.spill.percent",(float)0.8);
		final float recper = job.getFloat("io.sort.record.percent",(float)0.05);
		final int sortmb = job.getInt("io.sort.mb", 100);
		if (spillper > (float)1.0 || spillper < (float)0.0) {
			throw new IOException("Invalid \"io.sort.spill.percent\": " + spillper);
		}
		if (recper > (float)1.0 || recper < (float)0.01) {
			throw new IOException("Invalid \"io.sort.record.percent\": " + recper);
		}
		if ((sortmb & 0x7FF) != sortmb) {
			throw new IOException("Invalid \"io.sort.mb\": " + sortmb);
		}
		//sorter = ReflectionUtils.newInstance(
		//	job.getClass("map.sort.class", QuickSort.class, IndexedSorter.class), job);
		
		
		Util.pf("io.sort.mb = " + sortmb);
		// buffers and accounting
		int maxMemUsage = sortmb << 20;
		Util.pf("MMU = %d", maxMemUsage);
		
		int recordCapacity = (int)(maxMemUsage * recper);
		recordCapacity -= recordCapacity % RECSIZE;
		byte[] kvbuffer = new byte[maxMemUsage - recordCapacity];		
		
		
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: DbSliceInterestMain <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		String logfile = (args.length >= 2 ? args[1] : null);
		
		Map<String, String> optargs = Util.getClArgMap(args);
		Integer maxfile = (optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : Integer.MAX_VALUE);
		
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, true, new DbLookupSlicer(), new HadoopUtil.EmptyReducer(), null,
					a, a, a, a);			
			//HadoopUtil.alignJobConf(job, true, new DbLookupSlicer(), new HadoopUtil.EmptyReducer(), new SmartPartitioner(),
			//		a, a, a, a);
		}
		
		// checkStagingInfo(daycode);
		
		//System.exit(1);
		
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
			
			Util.pf("\nFound %d Big Data Log Files", pathlist.size());
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
				
		Path outputPath = new Path(Util.sprintf("/userindex/dbslice/%s/", daycode));
		fSystem.delete(outputPath, true);
		Util.pf("\nUsing directory %s", outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);			
		
		Util.pf("\nCalling TESTMEM DbSlice-Main for %s", daycode);
		
		// Specify various job-specific parameters     
		// job.setJobName(getJobName(daycode));
		job.setJobName("DB TEST MEM");
		
		Util.pf("Running DbSlice, memory info is : ");
		Util.showMemoryInfo();
		
		// This job is basically light on the reducers, shouldn't be an issue to use a lot of them.
		// job.setNumReduceTasks(listenCodeMap.size()); 
		//job.setPartitionerClass(SliceInterestSecond.SlicePartitioner.class);
		
		// Submit the job, then poll for progress until the job is complete
		RunningJob jobrun = JobClient.runJob(job);		
		
		
		return 0;
	}
	
	public static String getJobName(String daycode)
	{
		return Util.sprintf("SliceTest-Main %s", daycode);
	}
	
	public static class DbLookupSlicer extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		PathInfo relPath; 
		Path[] localFiles;
		int recCount = 0;
		
		
		@Override
		public void configure(JobConf job)
		{		
			Util.pf("\nConfiguring -----------------------------------");
			Util.pf("\nConfiguring -----------------------------------");
			
			/*
			try { 
				localFiles = DistributedCache.getLocalCacheFiles(job); 
			
				FileSystem fsys = FileSystem.get(job);
				LookupPack.initSing(fsys, "2012-04-09");
				
				Util.pf("Finished initializing LookupPack\n");
				Util.showMemoryInfo();
			}
			catch (IOException ioex) { 
				ioex.printStackTrace();
			}
			*/
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
			Util.massert(LookupPack.isReady());
			*/
			
			String logline = value.toString();
			
			BidLogEntry ble = BidLogEntry.getOrNull(relPath.pType, relPath.pVers, logline);
			if(ble == null)
				{ return; }
			
			// TODO: use fast field lookup here, don't bother splitting 
			String wtpid = ble.getField("wtp_user_id").trim();
			
			WtpId shortwtp = WtpId.getOrNull(wtpid);
			
			if(shortwtp == null)
				{ return; }
			
			Util.pf("ID is %s\n", shortwtp);
			
			/*
			for(String listcode : LookupPack.getSing().lookupId(shortwtp))
			{ 
				//BidLogEntry minimal = ble.transformToVersion(LogType.UIndexMinType, LogVersion.UIndexMinVers);
				
				// Okay, the followup job is going to partition by pixel id, and then sort.
				String outputkey = Util.sprintf("%s%s%s", listcode, Util.DUMB_SEP, wtpid);
				//String pixstr = padLeadingZeros(pix, 8);
				
				//output.collect(new Text(outputkey), new Text(minimal.getLogLine()));
				output.collect(new Text(outputkey), new Text(ble.getLogLine()));						
				
				//output.collect(new Text(wtpid), new Text(ble.getLogLine())); 
			}			
			*/
			
			recCount++;
			if((recCount  % 1000) == 0)
			{
				Util.pf("Finished initializing LookupPack\n");
				Util.showMemoryInfo();
				System.gc();	
			}			
		}
	}		
	
	// Send each batch of pixel information to its own partition
	/*
	public static class SmartPartitioner implements Partitioner<Text, Text>
	{
		Map<String, Integer> codemap;
		
		@Override
		public void configure(JobConf jobconf) {
			codemap = UserIndexUtil.getListenCodeMap(jobconf);
			//Util.pf("\nFound listen code map %s", codemap);
		}
		
		public int getPartition(Text key, Text value, int numPart)
		{
			// Local mode
			if(numPart == 1)
				{ return 0; }
			
			String[] pix_wtp = key.toString().split(Util.DUMB_SEP);	
			return codemap.containsKey(pix_wtp[0]) ? codemap.get(pix_wtp[0]) : 0;
		}
	}	
	*/
}
