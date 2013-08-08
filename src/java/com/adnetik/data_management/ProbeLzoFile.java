
package com.adnetik.data_management;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.DbUtil.*;
import com.adnetik.shared.BidLogEntry.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class ProbeLzoFile extends Configured implements Tool 
{
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	} 
	
	public int run(String[] args) throws IOException
	{
		
		String daycode;
		ExcName excname;
		LogType logtype;
		
		try {
			logtype = LogType.valueOf(args[0]);			
			excname = ExcName.valueOf(args[1]);
			daycode = args[2];
			
		} catch (Exception ex ) {
			
			Util.pf("Usage: ProbeLzoFile <daycode> <excname> <logtype>\n");
			return 1;
		}
		
		
		

		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Configure file to use LZO
		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		
		
		// Align Job
		{
			Text a = new Text("");	
			LongWritable lwrite = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new MyMapper(), new HadoopUtil.CountReducer(), a, a, a, lwrite);		
		}		
		
		FileSystem fsys = FileSystem.get(new Configuration());
		Path inpath = HadoopUtil.findHdfsLzoPath(fsys, excname, logtype, daycode);
		FileInputFormat.setInputPaths(job, new Path[] { inpath });	
		Util.pf("Input path is %s\n", inpath);
		
		// FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
				
		// Deal with output path
		{
			Path outputpath = new Path(Util.sprintf("/home/burfoot/probelzo/probe_%s_%s_%s.txt",
				excname, logtype, daycode));
			
			Util.pf("\nTarget Output path is %s", outputpath);
			HadoopUtil.moveToTrash(this, outputpath);
			FileOutputFormat.setOutputPath(job, outputpath);	
		}
		
		job.setJobName(Util.sprintf("ProbeLzoFile for %s, %s, %s", excname, logtype, daycode));
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		

		return 0;
	}
	
	public static class MyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{		
		@Override
		public void map( LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter rep)
		throws IOException
		{
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v22, value.toString());
			if(ble == null)
				{ return; }
			
			LogField lf = LogField.user_hour;
			
			String uhour = ble.getField(lf);
			
			output.collect(new Text(uhour), HadoopUtil.TEXT_ONE);			
		} 
	}	
	
}
