
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


public class MemoryTest
{
	public static void main(String[] arGs) throws Exception
	{
		Map<WtpId, Set<String>> lookupmap = Util.treemap();
		Map<String, Integer> countmap = Util.treemap();

		for(StagingType onetype : UserIndexUtil.StagingType.values())
		{
			String infopath = onetype.toString() + "_info.txt";
			Scanner infoscan = new Scanner(new File(infopath));
			//int excount = DbSliceInterestMain.populateLookupMap(lookupmap, countmap, infoscan);
			infoscan.close();
			
			//Util.pf("populated for type %s, found %d exceptions, %d total ids\n", onetype, excount, lookupmap.size());
		}
		
		// System.gc();
		
		Util.showMemoryInfo();
		int totcount = 0;
		
		for(String listcode : countmap.keySet())
		{
			totcount += countmap.get(listcode);
			Util.pf("Found %d ids for list code %s\n", countmap.get(listcode), listcode);	
		}
		
		Util.pf("Total number of IDS is %d", totcount);
	}
	
	
}
