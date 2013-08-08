/**
 * 
 */
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.shared.BidLogEntry.*;


public class PatternTest
{

	public static void main(String[] args) throws Exception
	{
		FileSystem fsys = FileSystem.get(new Configuration()); 
		String mypatt = "/userindex/dbslice/*/pixel_8181.slice";
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, mypatt);
		Util.pf("Found %d paths for pattern%s\n", pathlist.size(), mypatt);
		 
		for(Path onepath : pathlist)
		{
			Util.pf("Path is %s\n", onepath);
			
		}
	}

}
