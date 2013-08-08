
package com.adnetik.adhoc;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


import com.adnetik.shared.*;

public class TestHdfs2S3 
{
	public static void main(String args[])  throws Exception
	{
		Util.pf("hello, TestHdfs\n");
		
		Configuration conf = new Configuration();
		
		conf.setStrings("fs.default.name", "s3n://exelate-incoming/");
		
		Iterator<Map.Entry<String, String>> entry_it = conf.iterator();
		
		while(entry_it.hasNext())
		{
			Map.Entry<String, String> kv = entry_it.next();
			Util.pf("%s = %s\n", kv.getKey(), kv.getValue());	
		}
		
		FileSystem fsys = FileSystem.get(conf);
		
		
		
		Path s3path = new Path("s3n://exelate-incoming/exelate_20130118_00.tsv.gz");
		
		if(fsys.exists(s3path))
		{
			Util.pf("File exists\n");	
			
		}
		
	}
}

