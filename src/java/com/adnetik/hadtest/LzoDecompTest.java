
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

// This is the stuff we need to do this.
import com.hadoop.compression.lzo.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.LogType;
import com.adnetik.shared.Util.ExcName;


public class LzoDecompTest extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		FileSystem fsys = FileSystem.get(getConf());		
		InputStream raw = fsys.open(new Path("/data/click/yahoo_2012-02-03.lzo"));
		
		
		
		InputStream decomp = (new LzopCodec()).createInputStream(raw);
		
		BufferedReader bread = new BufferedReader(new InputStreamReader(decomp, BidLogEntry.BID_LOG_CHARSET)); 	
	
		for(String s = bread.readLine(); s != null; s = bread.readLine())
		{
			Util.pf("\nLine is %s", s);	
			
		}
		
		
		return -1;
	}
}
