	
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
	
public class AbstractMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
{
	LineFilter lFilter;	
	
	// TODO: put in logic to figure out LogType and LogVersion from the Reporter,
	// not from job configuration
	
	public static abstract class LineFilter
	{
		public LogType reltype;
		
		public void subConfigure(JobConf conf) { 
			
			String imptype = conf.get("LOGTYPE");
			
			//reltype = LogType.valueOf(imptype);
		}
		
		public abstract String[] filter(String line);
		
		// Subclasses override to modify path behavior
		public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
		{
		}
	}	
		
	public static abstract class LineAcceptor extends LineFilter
	{
		public abstract boolean accept(String line);
		
		public String[] filter(String line)
		{
			if(accept(line))
			{
				String[] toks = line.split("\t");
				return new String[] { toks[0], Util.joinButFirst(toks, "\t") };
			}
			
			return null;
		}
	}
		
	
	
	@Override
	public void configure(JobConf job)
	{
		//Util.pf("\nCONFIGURING -----------------------");
		
		try { 
			String filterClass  = job.get("FILTER_CLASS");
			lFilter = (LineFilter) Class.forName(filterClass).newInstance();
			lFilter.subConfigure(job);
			
		} catch (Exception ex) {
			
			Util.pf("\nError configuring filter");
			throw new RuntimeException(ex);
			
		}
	}
	
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
	throws IOException
	{			
		String[] result = lFilter.filter(value.toString());
		
		//Util.pf("\nrResult: %s, %s", result[0], result[1]);
		
		if(result != null)
			{ output.collect(new Text(result[0]), new Text(result[1])); }
	}
}

