
package com.adnetik.hadtest;

import java.io.*;
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

//org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,
//org.apache.hadoop.io.compress.BZip2Codec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec


import com.adnetik.shared.*;

public class HashCodeTest
{
	public static void main(String[] args) throws Exception
	{
		
		String wtpid = "000f441a-23eb-4f2f-b59f-6133d769287a";
		String ctry = "BR";
		
		for(String oneday : TimeUtil.getDateRange(6))
		{
			String lstr = oneday + ctry + wtpid;
			int hc = lstr.hashCode();	
			Util.pf("Hashcode for lstr=%s is %d\n", lstr, hc);
		}
		
	}
	
	//private int newHash(String prevhash, 
	
}
