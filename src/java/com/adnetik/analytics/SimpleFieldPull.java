
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

public class SimpleFieldPull extends Configured implements Tool
{
	protected Map<String, String> optArgs = Util.treemap();
	protected Set<Path> pathset = Util.treeset();
	
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new SimpleFieldPull(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		Util.putClArgs(args, optArgs);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());		

		// Set the input paths, either a manifest file or a glob path
		{
			pathset.addAll(HadoopUtil.pathListFromClArg(getConf(), args[0]));
			
			//addClPathInfo(pathset, getConf(), optArgs);
			
			Util.pf("\nFound %d total input paths\n", pathset.size());
			
			// Deal with extensions. Can only use one type of extension per job
			{
				Set<String> extSet = HadoopUtil.getExtensionSet(pathset);
				Util.pf("Extension set is %s\n", extSet);
				Util.massert(extSet.size() == 1, "Too many extensions in input %s", extSet);
				if(extSet.contains("lzo"))
				{
					job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
				}
			}
			
			// Find the logtype
			SortedSet<String> logtypeSet = Util.treeset();
			for(Path p : pathset)
			{
				for(String ltypePlus : LogEntry.LOGTYPE_PLUS_PIXEL)
				{
					if(p.toString().indexOf(ltypePlus) > -1)
						{ logtypeSet.add(ltypePlus); }
				}
			}
			
			Util.pf("Logtype Set is %s\n" , logtypeSet);
			Util.massert(logtypeSet.size() == 1);
			job.setStrings("LOG_TYPE", logtypeSet.first());
					
			FileInputFormat.setInputPaths(job, pathset.toArray(new Path[] {} ));		
		}

		{
			Text a = new Text("");
			HadoopUtil.alignJobConf(job, new PullMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);	
		}
		
		// Set the field list, send to the mapper
		{
			String[] fields = args[1].split(",");
			Util.pf("Found %d fields %s\n", fields.length, args[1]);
			job.setStrings("FIELD_LIST", args[1]);
		}
		
		// Set the job code and output path
		{
			String jobcode = args[2];
			String outputPath = HadoopUtil.smartRemovePath(this, jobcode);
			FileOutputFormat.setOutputPath(job, new Path(outputPath));	
		}	
		
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	

		return 0;
	}
		
	public static class PullMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		LogType relType = LogType.conversion;
		boolean isPixel = false;
		
		List<LogField> _fieldList = Util.vector();
		
		@Override
		public void configure(JobConf job)
		{
			//Util.pf("\nCONFIGURING -----------------------");

			String flist  = job.get("FIELD_LIST");
			
			for(String onef : flist.split(","))
				{ _fieldList.add(LogField.valueOf(onef)); }
			
			String logtype = job.get("LOG_TYPE");
			if("pixel".equals(logtype))
				{ isPixel = true; }
			else
				{ relType = LogType.valueOf(logtype); }
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{		
			LogEntry lent = null;
			
			Util.massert(false, "Need to reimplement with new BidLogEntry interface");
			
			/*
			if(isPixel)
				{ lent = new PixelLogEntry(value.toString());	}
			else
				{ lent = BidLogEntry.getOrNull(relType, value.toString()); }
			
				*/
				
			if(lent == null)
				{ return; }

			StringBuffer sb = new StringBuffer();
			
			for(int i = 1; i < _fieldList.size(); i++)
			{
				sb.append(lent.getField(_fieldList.get(i)).trim());	
				
				if(i < _fieldList.size()-1)
					{ sb.append("\t"); }
			}
			
			output.collect(new Text(lent.getField(_fieldList.get(0))), new Text(sb.toString()));
		}
	}
}

