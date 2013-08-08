
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


public class DailyTrackingFileSlice extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new DailyTrackingFileSlice(), args);
		System.exit(exitCode);
	}
		
	public int run(String[] args) throws Exception
	{		
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// TODO: copy to local, and smart autoremove feature
		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);		
		
		{
			Text a = new Text("");			
			IntWritable b = new IntWritable(1);
			HadoopUtil.alignJobConf(job, new DtfsMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}
		
		String dayCode = args[0];
		if("yest".equals(dayCode))
			{ dayCode = TimeUtil.getYesterdayCode(); }
		
		job.setStrings("DAY_CODE", dayCode);
		job.setJobName(Util.sprintf("Tracking File Slice %s", dayCode));

		List<Path> pathlist = HadoopUtil.getImpTrackFiles();
		Path[] testpath = new Path[2];
		
		for(int i = 0; i < testpath.length; i++)
			{ testpath[i] = pathlist.get(i); }		
		
		//FileInputFormat.setInputPaths(job, testpath);		
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[]{}));			
		
		Path tempOutput = HadoopUtil.classSpecTempPath(this);
		HadoopUtil.checkRemovePath(this, tempOutput);
		FileOutputFormat.setOutputPath(job, tempOutput);
		
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		// Clean up and remove
		{
			FileSystem fsys = FileSystem.get(job);
			fsys.mkdirs(new Path("/data/analytics/TRK_SLICE/"));
			Path destFilePath = HadoopUtil.getSlicePath(dayCode);
			Path tempPartPath = new Path(Util.sprintf("%s/part-00000", tempOutput));
			
			fsys.rename(tempPartPath, destFilePath);
			fsys.delete(tempOutput, true);
		}
		
		
		return 0;
	}

	public static class DtfsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{				
		
		Map<String, String> combConvMap = Util.treemap();
		
		@Override
		public void configure(JobConf job) 
		{
			try {
				FileSystem fsys = FileSystem.get(job);
			
				String dayCode = job.get("DAY_CODE");
				
				String convDataPath = Util.sprintf("/data/analytics/convdata/convert_%s.txt", dayCode);
				
				Path cdp = new Path(convDataPath);
				
				Scanner sc = new Scanner(fsys.open(cdp));
				
				while(sc.hasNextLine())
				{
					String line = sc.nextLine();
					String[] toks = line.split("\t");
					String[] subtoks = toks[0].split("____");
					
					String combId = UpdateTrackFile.createCombineKey(subtoks[0], Integer.valueOf(subtoks[1]));
										
					combConvMap.put(combId, toks[1]);	
				}
				sc.close();
				
			} catch (IOException ioex) {
				
				ioex.printStackTrace();	
			}
		}
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			// TODO: does there need to be timestamp logic here?	
			String[] toks = value.toString().split("\t");
			
			if(combConvMap.containsKey(toks[0]))
			{
				String v = Util.sprintf("conv_ts=%s\t%s",
						combConvMap.get(toks[0]), Util.joinButFirst(toks, "\t"));
				
				output.collect(new Text(toks[0]), new Text(v));
			}
		}
	}
}
