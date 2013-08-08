
package com.adnetik.analytics;

import java.io.IOException;
import java.util.*;

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
import com.adnetik.shared.Util.*;

public class AbstractScanJob extends Configured implements Tool
{
	protected Map<String, String> optArgs = Util.treemap();
	protected Set<Path> pathSet = Util.treeset();
	
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		Util.putClArgs(args, optArgs);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());		

		
		String jobCode;
		{
			String filterClass = args[0];
			Object myfilter = Class.forName(filterClass).newInstance();
			
			((AbstractMapper.LineFilter ) myfilter).modifyPathSet(getConf(), pathSet);

			job.setStrings("FILTER_CLASS", filterClass);
			jobCode = myfilter.getClass().getSimpleName();
		}		

		
		

		// Set the input paths, either a manifest file or a glob path
		{			
			addClPathInfo(pathSet, getConf(), optArgs);
			
			Util.pf("\nFound %d total input paths", pathSet.size());
			
			// Deal with extensions. Can only use one type of extension per job
			{
				Set<String> extSet = HadoopUtil.getExtensionSet(pathSet);
				Util.pf("\nExtension set is %s", extSet);
				Util.massert(extSet.size() == 1, "Too many extensions in input %s", extSet);
				if(extSet.contains("lzo"))
				{
					job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
				}
			}
			
			// Subclass to add/subtract paths (e.g. no Google)
			modifyPathSet(); 
			
			FileInputFormat.setInputPaths(job, pathSet.toArray(new Path[] {} ));		
		}

		doAlignJob(job);


		
		String outputPath = HadoopUtil.smartRemovePath(this, jobCode);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
	
		// Specify various job-specific parameters    
		job.setJobName(Util.sprintf("AbstractScanJob %s", jobCode));

		if(optArgs.containsKey("logtype")) 
		{
			job.setStrings("LOGTYPE", optArgs.get("logtype"));
		}
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		
		return 0;
	}
		
	void doAlignJob(JobConf job)
	{
		String redcode = optArgs.containsKey("reducer") ? optArgs.get("reducer") : "noop";
		Text a = new Text("");	
		LongWritable lw = new LongWritable(0);
		
		
		if("noop".equals(redcode))
		{
			HadoopUtil.alignJobConf(job, new AbstractMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);			
		} else if("count".equals(redcode)) {
			HadoopUtil.alignJobConf(job, new AbstractMapper(), new HadoopUtil.CountReducer(), a, a, a, lw);
		} else if("set".equals(redcode)) {
			HadoopUtil.alignJobConf(job, new AbstractMapper(), new HadoopUtil.SetReducer(), a, a, a, a);	
			Util.pf("\nUsing Set Reducer");
		} else if("setcount".equals(redcode)) {
			HadoopUtil.alignJobConf(job, new AbstractMapper(), new HadoopUtil.SetCountReducer(), a, a, a, lw);	
			Util.pf("\nUsing SetCountReducer");
		} else {
			try {
				Util.pf("\nDynamic reducer name is %s", redcode);
				Object red = Class.forName(redcode).newInstance();
				Reducer<Text, Text, Text, Text> dynred = Util.cast(red);
				HadoopUtil.alignJobConf(job, new AbstractMapper(), dynred, a, a, a, a);	
			} catch (Exception ex) {
				Util.pf("\nError configuring reducer\n");
				throw new RuntimeException(ex);
			}

		}
		
		if(optArgs.containsKey("numreduce"))
		{
			int numred = Integer.valueOf(optArgs.get("numreduce"));
			job.setNumReduceTasks(numred);
			Util.pf("\nSet number of reduce tasks to %d", numred);
		}
	}
	
	public static void addClPathInfo(Set<Path> inputSet, Configuration conf, Map<String, String> optArgs) throws IOException
	{
		List<Path> addlist = HadoopUtil.extraClPathArgs(conf, optArgs);
		List<Path> remlist = HadoopUtil.removeClPathArgs(conf, optArgs);
		
		Util.pf("\nFound %d add, %d removes", addlist.size(), remlist.size());
		
		if(addlist.size() > 0)
		{
			int prevsize = inputSet.size();
			inputSet.addAll(addlist);
			Util.pf("\nFound %d extra CL paths, added %d of them", addlist.size(), inputSet.size()-prevsize);
		}
		
		if(remlist.size() > 0)
		{
			int prevsize = inputSet.size();
			inputSet.removeAll(remlist);
			Util.pf("\nFound %d to-remove CL paths, subtraced %d of them", remlist.size(), prevsize-inputSet.size());			
		}
	}
	
	// Subclasses override me
	protected void modifyPathSet() { }
}

