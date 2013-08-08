
package com.adnetik.data_management;

import java.io.IOException;
import java.io.Console;
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
//import com.adnetik.shared.BidLogEntry.LogType;


public class GenerateConversionFile extends Configured implements Tool
{			
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new GenerateConversionFile(), args);
		System.exit(exitCode);
	}
		
	public int run(String[] args) throws Exception
	{	
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());		
		
		// FileSystem	
		FileSystem fsys = FileSystem.get(getConf());

		String dayCode = args[0];
		
		if("yest".equals(dayCode))
		{
			dayCode = TimeUtil.getYesterdayCode();	
		}
		
		// Align classes
		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}		
		
		job.setJobName(Util.sprintf("Generate Conversion File %s", dayCode));
		
		
		List<Path> pathlist = HadoopUtil.findLzoFiles(fsys, null, LogType.conversion, dayCode);
		
		// TODO: change to smartRemovePath?
		Path tempOutputPath = HadoopUtil.classSpecTempPath(this);
		fsys.delete(tempOutputPath, true);
		
		Util.pf(tempOutputPath.toString());
		
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));			
		FileOutputFormat.setOutputPath(job, tempOutputPath);	
		
		JobClient.runJob(job);	
		
		// Copy to real destination 
		{
			Path destPath = new Path(Util.sprintf("/data/analytics/convdata/convert_%s.txt", dayCode));
			Path partPath = new Path(Util.sprintf("%s/part-00000", tempOutputPath));
			
			fsys.rename(partPath, destPath);
			fsys.delete(tempOutputPath, true);
		}
		
		return 0;
	}
	
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			try {
				BidLogEntry ble = new BidLogEntry(LogType.conversion, value.toString());
				int campId = ble.getIntField("campaign_id");
				String wtpId = ble.getField("wtp_user_id");
				String tstamp = ble.getField("date_time");
				
				
				// TODO: what if WTP is not set?
				try { BidLogEntry.checkWtp(wtpId); }
				catch (Exception ex) {
					//System.out.printf("\nInvalid wtpID=%s", wtpId);	
					return;
				}
				
				String combKey = Util.sprintf("%s____%d", wtpId, campId);
				output.collect(new Text(combKey), new Text(tstamp));
				
			} catch (BidLogFormatException ble) {
				
				ble.printStackTrace();
			}
		}
	}	
}
