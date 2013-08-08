
package com.adnetik.hadtest;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;


public class ExamineInputSplits extends Configured implements Tool
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

		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new HadoopUtil.EmptyMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}
		
		{
			String patt = "/data/imp/rtb_2012-01-28.lzo";
			
			//String patt = "/data/no_bid/2012-01-31/rtb/*.log.gz";
			List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), patt);	
			
			Util.pf("\nFound %d paths", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));	
		}
		
		InputSplit[] splits = job.getInputFormat().getSplits(job, 20);
		
		for(InputSplit isp : splits)
		{
			Util.pf("\nInput split type is %s", isp);
			
			
		}
		
		Util.pf("\nNumber of input splits is %d\n", splits.length);
		
		
		return 0;
	}
		
}
