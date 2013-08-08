package com.adnetik.bm_etl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.*;

import com.adnetik.shared.*;

public class AdnetikReducerAll extends MapReduceBase implements Reducer<Text, Metrics, Text, Text> 
{
	@Override
	public void configure(JobConf job)
	{
		// Transform from String to DimCodes
		String daycode = job.get("DAY_CODE");
		
		//Util.pf("\nDimension Set is %s", dimSetInfo);
				
		try  { 
			// TODO: do we even have to do this? Probably not, right?
			// CatalogUtil.initSing(daycode);
					
		} catch (Exception ex) {
			// Basically fucked here
			throw new RuntimeException(ex);
		}
 	}	

 	@Override
	public void reduce(Text key, Iterator<Metrics> values, OutputCollector<Text, Text> collector, Reporter reporter) 
	throws IOException
	{		
		Metrics result = new Metrics();

		while(values.hasNext())
		{
			result.add(values.next());	
		}

		collector.collect(key, new Text(result.toString("&")));
	}
}
