
package com.adnetik.data_management;

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

public class TestCronOutput extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new TestCronOutput(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		FileSystem fsys = FileSystem.get(getConf());
		
		// Test reading a file from HDFS
		String hadoopFile = "/mnt/burfoot/tools/hadoopfile.txt";
		
		List<String> datalines = HadoopUtil.readFileLinesE(fsys, hadoopFile);
		
		Util.pf("\nHadoop file data is:");
		for(String s : datalines)
		{
			Util.pf("\n\t%s", s); 
		}
		
		return 0;
	}
}
