
package com.adnetik.analytics;

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


public class UpdateTrackFile extends Configured implements Tool
{
	private static String HDFS_TRK_DIR = HadoopUtil.IMP_TRK_DIR;
	public static String TEMP_OUTPUT = HDFS_TRK_DIR + "-tmp-out";
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new UpdateTrackFile(), args);
		System.exit(exitCode);
	}
		
	public int run(String[] args) throws Exception
	{		
		if(args.length != 1)
		{
			System.out.printf("\nUsage: UpdateTrackFile <daycode|yest>\n\n");
			return 1;
		}		
		
		String daycode = args[0];
		
		if("yest".equals(daycode))
		{
			daycode = TimeUtil.cal2DayCode(TimeUtil.getYesterday());	
		}
		
		if(!TimeUtil.checkDayCode(daycode))
		{
			System.out.printf("\nInvalid day code: %s", daycode);	
			return 1;
		}
		
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		job.setJobName("Update Impression Tracking File - " + daycode);

		// Class alignments
		{
			Text t = new Text("");			
			HadoopUtil.alignJobConf(job, new TrackImpMapper(), new TrackImpReducer(), t, t, t, t);
		}
	
		// Need to delete the tmp-output dir, otherwise job will not run, grr
		// This should normally be done by the previous day's job, but it may have failed.
		{
			Path tmpOutputPath = new Path(TEMP_OUTPUT);			
			FileSystem fsys = FileSystem.get(getConf());
			boolean diddel = fsys.delete(tmpOutputPath, true);
			
			if(diddel)
			{
				System.out.printf("\nDeleted temporary directory %s, error on previous run?", tmpOutputPath);
			}
		}
		
		FileInputFormat.setInputPaths(job, getInputCheckPaths(daycode));			
		FileOutputFormat.setOutputPath(job, new Path(TEMP_OUTPUT));	
	
		// I don't know why these calls aren't more consistent. Shouldn't it just be 
		// job.setOutputFormat(SomeLzoOutputFormat.class)?		
		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		job.setNumReduceTasks(50);

		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		cleanUpDirs();
	
		return 0;
	}
	
	void cleanUpDirs() throws IOException
	{
		FileSystem fsys = FileSystem.get(getConf());
		fsys.delete(new Path(HDFS_TRK_DIR), true);
		fsys.rename(new Path(TEMP_OUTPUT), new Path(HDFS_TRK_DIR));
	}
	
	Path[] getInputCheckPaths(String daycode) throws IOException
	{
		FileSystem fsys = FileSystem.get(getConf());		
		List<Path> ipaths = Util.vector();
		
		for(ExcName ename : ExcName.values())
		{
			String p = HadoopUtil.getHdfsLzoPath(ename, LogType.imp, daycode);
			
			if(fsys.exists(new Path(p)))
			{
				System.out.printf("\nFound path %s", p);
				ipaths.add(new Path(p));
			}
		}
		
		// Add all the part-00000 files in the tracking directory.
		for(int tid = 0; tid < 1000; tid++)
		{
			String tidpath = Util.sprintf("%s/part-%s.lzo", HDFS_TRK_DIR, HadoopUtil.pad5(tid));
			Path trkpath = new Path(tidpath);
			
			if(fsys.exists(trkpath))
			{
				//System.out.printf("\nFound tracking file %s", tidpath);
				ipaths.add(trkpath);	
			} else {
				
				System.out.printf("\nAdded %d tracking files, could not find file %s", tid, trkpath);
				break;	
			}
		}
		
		for(Path testpath : ipaths)
		{
			if(!fsys.exists(testpath))
			{
				System.err.printf("\nWarning: path does not exist: %s", testpath);	
			}
		}
		
		return ipaths.toArray(new Path[] { } );
	}

	public static class TrackImpMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		static String[] ADD_FIELDS = new String[] { "date_time", "line_item_id" };
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			// Okay, two different cases here.
			String line = value.toString();
			String[] toks = line.split("\t");
			StringBuffer sb = new StringBuffer();
			String combId = null;
			
			if(toks.length < 20)
			{
				// We are dogfeeding: the input is the output of a previous
				// day's result.
				combId = toks[0];

				for(int i = 1; i < toks.length; i++)
				{
					sb.append(toks[i]);
					
					if(i < toks.length-1)
						sb.append("\t");
				}				
				
			} else {
				
				// TODO: figure out how to check which log file type we're dealing with
				BidLogEntry ble;
				try { 
					ble = new BidLogEntry(LogType.conversion, value.toString());				
				} catch (BidLogEntry.BidLogFormatException blex) {
					return;	
				}		
				
				
				String wtpId = ble.getField("wtp_user_id");
				int campId = ble.getIntField("campaign_id");
				
				combId = createCombineKey(wtpId, campId);
				
				for(int i = 0; i < ADD_FIELDS.length; i++)
				{
					sb.append(ble.getField(ADD_FIELDS[i]));
					
					if(i < ADD_FIELDS.length-1)
						sb.append("\t");
				}
			
			}
			
			output.collect(new Text(combId), new Text(sb.toString()));
		}
	}

	public static class TrackImpReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		SortedSet<String> checkSet = Util.treeset();
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			// TODO: to make this re-entrant, need to check if the 
			// impression_id was already seen.
			
			checkSet.clear();
			
			while(values.hasNext())
			{
				checkSet.add(values.next().toString());	
			}
			
			for(String line : checkSet)
			{
				collector.collect(key, new Text(line));
			}
		}		
	}		

	public static String createCombineKey(String wtpId, int campId)
	{
		return Util.sprintf("%s___%d", wtpId, campId);
	}

}
