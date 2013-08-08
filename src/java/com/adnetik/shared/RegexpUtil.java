
package com.adnetik.shared;

import java.util.*;
import java.io.*;
import java.util.regex.*;
import java.util.zip.GZIPInputStream;


// mini change 
// another change

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.text.SimpleDateFormat;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

 
public class RegexpUtil
{
	// eg hdfs://heramba-ganapati/data/click/admeld_2012-02-27.lzo:0+15829535
	public static Pattern HDFS_PATH = Pattern.compile(".*/data/(.*)/(.*)_(.*).lzo.*");
	
	public static HdfsPathBag getPathBag(String path)
	{
		Matcher m = HDFS_PATH.matcher(path);
		
		if(!m.matches())
			{ return null; }
		
		HdfsPathBag hpb = new HdfsPathBag();
		
		hpb.ltype = LogType.valueOf(m.group(1));
		hpb.exc = ExcName.valueOf(m.group(2));
		hpb.daycode = m.group(3);
		hpb.lvers = Util.targetVersionFromDayCode(hpb.daycode);
		return hpb;
	}
	
	public static class HdfsPathBag
	{
		public LogType ltype;
		public LogVersion lvers;
		public ExcName exc;		
		public String daycode;
		
		public String toString() 
		{
			return Util.sprintf("Type=%s, Vers=%s, Exc=%s, Daycode=%s", 
				ltype, lvers, exc, daycode);
		}
	}

        public static String findDayCode(String targ)
        {
        	java.util.regex.Pattern dcpatt = java.util.regex.Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d");
        	
        	java.util.regex.Matcher m = dcpatt.matcher(targ);
        	
        	if(!m.find())
        		{ return null; }
        	
        	return m.group();
        }
	
	public static void main(String[] args)
	{
		List<String> pathlist = Util.vector();
		pathlist.add("/data/imp/rtb_2012-02-15.lzo");
		pathlist.add("/data/click/rtb_2012-02-15.lzo");
		pathlist.add("hdfs://heramba-ganapati/data/click/admeld_2012-02-27.lzo:0+15829535");
		
		for(String onepath : pathlist)
		{
			HdfsPathBag hpb = getPathBag(onepath);
			Util.pf("\n Path is %s \n\t %s", onepath, hpb);			
		}
		
		Util.pf("\n");		
	}
}

