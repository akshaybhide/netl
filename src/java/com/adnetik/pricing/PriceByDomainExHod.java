
package com.adnetik.pricing;

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


public class PriceByDomainExHod extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
		
	public int run(String[] args) throws Exception
	{		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());				
		
		job.setJobName(Util.sprintf("Price By Domain Ex Hod"));

		
		// Find the conversion paths within the given window.
		String daycode = "2012-01-05";
		
		{
			String pathpatt = Util.sprintf("/data/imp/*%s.lzo", daycode);
			//String pathpatt = "/data/imp/casale_2012-01-05.lzo";
			List<Path> pathlist = HadoopUtil.getGlobPathList(fSystem, pathpatt);
			Util.pf("\nFound %d impression paths", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
		}
		
		{
			Text t = new Text("");			
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.DoubleCountReducer(), t, t, t, t);
		}

		String outputPath = HadoopUtil.smartRemovePath(this, "dexhood");
		System.out.printf("\nOutput path is %s", outputPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		

		return 0;
	}

	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			//Configuration conf = JobContext.getConfiguration();
			
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, value.toString());
			if(ble == null)
				{ return; }
			
			double wincpm = Util.getWinnerPriceCpm(ble);
			
			String adex = ble.getField("ad_exchange");
			String hour = ble.getField("hour");
			String domain = ble.getField("domain");
			
			String totkey = Util.sprintf("%s%s%s%s%s__exp1", adex, Util.DUMB_SEP, hour, Util.DUMB_SEP, domain);
			String reskey = Util.sprintf("%s%s%s%s%s__expx", adex, Util.DUMB_SEP, hour, Util.DUMB_SEP, domain);
			
			//Util.pf("\nFound key of %s , wincpm = %.03f", reskey, wincpm);

			output.collect(new Text(totkey), HadoopUtil.TEXT_ONE);
			output.collect(new Text(reskey), new Text("" + wincpm));
		}
	
	}

}
