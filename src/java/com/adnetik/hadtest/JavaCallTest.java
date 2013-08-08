
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.net.*;

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
import org.apache.hadoop.filecache.*;


import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;
// import com.adnetik.analytics.InterestUserUpdate;
import com.adnetik.userindex.UserIndexUtil.StagingType;
import com.adnetik.userindex.DbSliceInterestMain;


public class JavaCallTest
{
	public static void main(String[] arGs) throws Exception
	{
		Configuration conf = new Configuration();
		FileSystem fsys = FileSystem.get(conf);
		
		String mypath = "/userindex/staging/2012-05-07/*";
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, mypath);
	
		Util.pf("Found %d paths\n", pathlist.size());
		
		for(Path onepath : pathlist)
		{
			Util.pf("Path is %s\n", onepath.toString());	
			
			
		}
	}
	
	
}
