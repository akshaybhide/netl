
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

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.UserIndexUtil;


public class ModTheoryCheck
{	

	public static void main(String[] args)
	{
		Util.pf("Hello from ModTheory Check\n");
		
		checkForInt(345);
		
		Random jrand = new Random();
		
		for(int i : Util.range(10000))
		{
			int P = jrand.nextInt();
			P = (P < -1 ? -P : P);
			checkForInt(P);
			
		}
	}
	
	
	private static void checkForInt(int P)
	{
		int N = P % 1024;
		int M = P % 16;
		
		Util.massert(M == (N%16));
		
		Util.pf("Worked okay for P=%d\n", P);
	}
	
}
