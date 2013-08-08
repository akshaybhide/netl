
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;

public class MoveBmToExtern extends Configured implements Tool
{
	private static final String PREF_PATT = "hdfs://heramba-ganapati/mnt/data/burfoot/bm-etl/output/2012-02-16/";
	
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);	
		
	}
	
	public int run(String[] args) throws Exception
	{
		FileSystem fsys = FileSystem.get(getConf());
		String patt = Util.sprintf("/mnt/data/burfoot/bm-etl/output/*/part*");
		List<Path> pathlist = HadoopUtil.getGlobPathList(FileSystem.get(getConf()), patt);
		Set<Integer> lenset = Util.treeset();
		
		for(Path p : pathlist)
		{
			String spath = p.toString();
				
			String pref = spath.substring(0, PREF_PATT.length());
			String part = spath.substring(PREF_PATT.length());
			
			//Util.pf("\nPrefix is %s, partcode is %s\n%s", pref, part, spath);
			
			Path extpath = new Path(Util.sprintf("%s/extern/%s", pref, part));
			
			
			
			Util.pf("\nOld path - new path \n\t%s\n\t%s", spath, extpath);
			
			fsys.mkdirs(extpath.getParent());
			
			fsys.rename(p, extpath);
			
		}
		
		Util.pf("\nThe length set is %s", lenset);
		
		return 1;
		
	}
}
