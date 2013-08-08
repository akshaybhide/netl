
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

import com.adnetik.shared.Util;

public class TestRename extends Configured implements Tool
{

	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new TestRename(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// All the necessary info comes from a SINGLE comline argument, which is a file name
		// eg rtb_imp_2011-08-15.txt
		String manifestPath = args[0];
		String excName; String dayCode; String logType;
		{
			System.out.printf("\nMani file is %s", manifestPath);
			
			// Gotcha - argument to split(...) is interpreted as a regexp, not a string
			String subfile = manifestPath.split("\\.")[0];
			String[] toks = subfile.split("___");
			excName = toks[0];
			logType = toks[1];
			dayCode = toks[2];
		}
		
		System.out.printf("\nCalling LogSync for exchange=%s, log=%s, daycode=%s",
			excName, logType, dayCode);
		
		String outputFilePath = Util.sprintf("/data/%s/%s/%s-outdir",
							excName, logType, dayCode);
		
		System.out.printf("\nOutput path is %s\n", outputFilePath);
		
		// rename result
		{
			String src = Util.sprintf("%s/part-00000.lzo", outputFilePath);
			String dst = Util.sprintf("/data/%s/%s_%s.lzo", logType, excName, dayCode);
					
			System.out.printf("\nSrc/Dst paths are:\n\t%s\n\t%s", src, dst); 
			
			// Need to make sure the /data/<logtype> directory exists
			FileSystem fsys = FileSystem.get(getConf());
			fsys.mkdirs(new Path(Util.sprintf("/data/%s/", logType)));
			fsys.rename(new Path(src), new Path(dst));
			
			// Delete the directory
			// fsys.delete(new Path(outputFilePath), true);
		}
		
		return 0;
	}
}
