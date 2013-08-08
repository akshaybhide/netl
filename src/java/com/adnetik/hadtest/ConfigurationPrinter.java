
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;


public class ConfigurationPrinter extends Configured implements Tool
{
	
	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();
		
		for(Map.Entry<String, String> entry : conf)
		{
			System.out.printf("\n%s=%s", entry.getKey(), entry.getValue());	
		}
		
		return 0;	
	}
	
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitCode);
	}
	
}
