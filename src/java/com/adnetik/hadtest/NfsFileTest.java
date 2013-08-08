
package com.adnetik.hadtest;

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
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

public class NfsFileTest extends Configured implements Tool
{
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new NfsFileTest(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		String globpattern = args[0];

		//FileSystem local = FileSystem.getLocal(getConf());
		//List<Path> pathlist = HadoopUtil.getGlobPathList(local, globpattern);
		List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), globpattern);
		
		Util.pf("Found %d paths\n", pathlist.size());
		
		return 0;
	}
}

