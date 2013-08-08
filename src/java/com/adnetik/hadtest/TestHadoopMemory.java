
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.*;


// Just a dumb test of our ability to set memory for Hadoop on command line.
public class TestHadoopMemory extends Configured implements Tool
{
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new TestHadoopMemory(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		List<String> myset = Util.vector();
		
		Util.showMemoryInfo();		
		
		for(int i = 0; i < 4*8000000; i++)
		{
			myset.add(SortScrubCountryUrl.randomUserId());
		
			if((i % 1000000) == 0)
			{
				System.gc();
			}
		}
		
		System.gc();
		
		Util.showMemoryInfo();
		
		Random jrand = new Random();
		Util.pf("\nRandom string %s\n", myset.get(jrand.nextInt(myset.size())));
		
		return 1;
	}

	
}
