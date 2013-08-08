
package com.adnetik.hadtest;

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
import com.adnetik.data_management.*;

public class TestLogSync extends Configured implements Tool
{
	
	// TODO - we want to save more data than is listed here.
	private static final Set<Integer> bid_excluded = new HashSet<Integer>
		(Arrays.asList(new Integer[] {4,6,8,28,19,25,94,26,31,101,85,22,29,37,27,87} ));
	private static final Set<Integer> activity_excluded = new HashSet<Integer>
		(Arrays.asList(new Integer[] {4,6,10,30,21,27,96,28,33,103,87,24,31,39,29,89} ));
		
	public static class LogMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
	
		//private Text exName;
		//private Text log = new Text();
		
		//private static final Set<Object> excluded = new HashSet<Object>();
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			// Break log into fields for processing
			String[] fieldList = value.toString().split("\t");
			
			boolean isBidEx = (fieldList.length == 118);
						
			Set<Integer> excluded = (isBidEx ? bid_excluded : activity_excluded);

			StringBuffer sbuf = new StringBuffer();
			
			// Start at x=1 because the timestamp is the key
			for (int x=1; x<fieldList.length; x++)
			{
				String toapp = excluded.contains(x) ? "" : fieldList[x];
				sbuf.append(toapp);
				
				if(x < fieldList.length-1)
					{ sbuf.append("\t"); }
			}
		
			String adexName = fieldList[(isBidEx ? 7 : 9)];
			
			output.collect(new Text(adexName), new Text(sbuf.toString()));

		}
	}

	public static class EmptyReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			while(values.hasNext())
			{
				collector.collect(new Text(""), values.next());
				//collector.collect(key, values.next());
			}
			
		}		
	}		

	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new LogSync(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		getConf().setBoolean("mapred.output.compress", true);
		getConf().setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new LogMapper(), new EmptyReducer(), a, a, a, a);
		}
		
		// All the necessary info comes from a SINGLE comline argument, which is a file name
		// eg rtb_imp_2011-08-15.txt
		String manifestPath = args[0];
		ExcName excName; String dayCode; String logType;
		{
			System.out.printf("\nMani file is %s", manifestPath);
			
			// Gotcha - argument to split(...) is interpreted as a regexp, not a string
			String subfile = manifestPath.split("\\.")[0];
			String[] toks = subfile.split("___");
			excName = Util.excLookup(toks[0]);
			logType = toks[1];
			dayCode = toks[2];
		}
		
		System.out.printf("\nCalling LogSync for exchange=%s, log=%s, daycode=%s",
			excName, logType, dayCode);
		
		String outputFilePath = Util.sprintf("/tmp/%s/%s/%s-outdir", excName, logType, dayCode);
		String partFilePath = Util.sprintf("%s/part-00000.lzo", outputFilePath);
		String destFilePath = lzoFilePath(logType, excName, dayCode);
		
		//FileSystem fileSystem = FileSystem.get(getConf());		
		//if(fileSystem.

		System.out.printf("\nOutput path is %s\n", outputFilePath);
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("LogSync %s %s %s", excName, logType, dayCode));

		setInputFromManifest(job, manifestPath);
		//setInputNormal(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(outputFilePath));		
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);		
		
		// rename result
		{
			System.out.printf("\nSrc/Dst paths are:\n\t%s\n\t%s", partFilePath, destFilePath); 

			// Need to make sure the /data/<logtype> directory exists
			FileSystem fsys = FileSystem.get(getConf());
			fsys.mkdirs(new Path(Util.sprintf("/data/%s/", logType)));
			fsys.rename(new Path(partFilePath), new Path(destFilePath));
			
			// Delete the directory
			fsys.delete(new Path(outputFilePath), true);
		}
		
		return 0;
	}
	
	// TODO: Make ExcName and LogType enumerations for type safety
	static String lzoFilePath(String logType, ExcName excName, String dayCode)
	{
		return Util.sprintf("/data/%s/%s_%s.lzo", logType, excName, dayCode);
	}
			
	public void setInputNormal(JobConf job, String argz)
	{
		FileInputFormat.setInputPaths(job, new Path(argz));	
	}
	
	public void setInputFromManifest(JobConf job, String argz)
	{
		throw new RuntimeException("not implemented");
		//Path[] inputPaths = Util.getS3Paths(argz, Util.S3N_BUCKET_PREF);
		//FileInputFormat.setInputPaths(job, inputPaths);			
	}
}
