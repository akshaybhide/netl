
package com.adnetik.data_management;

import java.io.*;
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
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.BidLogEntry.*;

public class CommLineErrCheck
{
	BufferedReader bReader;
	LogType relType;
	LogVersion relVers;
	
	public CommLineErrCheck(String argz) throws IOException
	{
		relVers = LogVersion.v13;
		
		if(argz.endsWith(".gz"))
		{
			// FileMode
			relType = Util.logTypeFromNfsPath(argz);
			bReader = Util.getGzipReader(argz);
			
		} else {
			
			relType = LogType.valueOf(argz);
			bReader = new BufferedReader(new InputStreamReader(System.in));
		}		
		
		
	}
	
	void runCheck() throws IOException
	{
		int lc = 0;
		int errcount = 0;
		
		for(String line = bReader.readLine(); line != null; line = bReader.readLine())
		{
			lc++;
			
			if(line.trim().length() == 0)
				{ continue; }
			
			//Util.pf("\nLine is %s", line);
			
			try {
				BidLogEntry ble = new BidLogEntry(relType, relVers, line);
				ble.superStrictCheck();
			} catch (BidLogFormatException blfex) {
				
				int numtoks = line.trim().split("\t").length;
				
				System.err.printf("\nEncountered error %s on line %d, numtoks %d", blfex.e, lc, numtoks);
				System.err.printf("\nMessage %s", blfex.getMessage());
				System.out.printf("\n%s", line);
				//Util.pf(line);
				//Util.pf("\nLine is\n%s", line);
				errcount++;
				
			}
		}
		
		bReader.close();
		Util.pf("\nScanned %d lines, found %d errors", lc, errcount);		
	}
	
	public static void main(String[] args) throws Exception
	{
		if(args.length == 0)
		{
			Util.pf("\nUsage: CommLineErrCheck <filename|logtype>");	
			System.exit(1);
		}
		
		CommLineErrCheck clec = new CommLineErrCheck(args[0]);
		
		clec.runCheck();
	}
}
