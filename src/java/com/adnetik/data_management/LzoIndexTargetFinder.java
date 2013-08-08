
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class LzoIndexTargetFinder
{		

	public static void main(String[] args) throws IOException
	{
		String lzopatt = args[0];
		String outputfile = args[1];
		
		Util.massert(lzopatt.endsWith(".lzo"), 
			"Purpose of this code is to search for LZO files, must supply *.lzo wildcard, found %s", lzopatt);
		
		FileSystem fsys = FileSystem.get(new Configuration());
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, lzopatt);
		
		List<String> noindexlist = Util.vector();
		
		for(Path onepath : pathlist)
		{
			// Util.pf("Found pattern %s\n", onepath);	
			
			if(!hasLzoIndex(fsys, onepath))
			{
				Util.pf("No Index found for %s\n", onepath.toString());
				noindexlist.add(onepath.toString());
			}
		}
		
		FileUtils.writeFileLinesE(noindexlist, outputfile);
		
	}
	
	public static boolean hasLzoIndex(FileSystem fsys, Path lzopath) throws IOException
	{
		Util.massert(lzopath.toString().endsWith(".lzo"));
		
		Path lzoindex = new Path(lzopath.toString() + ".index");
		
		return fsys.exists(lzoindex);
		
	}
	
}
