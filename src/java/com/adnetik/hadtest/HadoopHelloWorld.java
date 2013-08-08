
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

public class HadoopHelloWorld extends Configured implements Tool
{

	public static void main(String[] args) throws Exception
	{
		String myline = "dan\tburfoot\tis\tcool";
		String subline = myline.substring(myline.indexOf("\t")+1);
		Util.pf("\nSubline is: \n%s", subline);
		
		
		//int exitCode = ToolRunner.run(new HadoopHelloWorld(), args);
		//System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		Util.pf("Hello, world from Hadoop\n\n");
		
		return 1;
	}
}
