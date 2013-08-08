/**
 * 
 */
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


public class TransformDataRep extends Configured implements Tool 
{
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{
		processOneDir("2012-02-25");
		return 0;
	}
	
	void processOneDir(String daycode) throws IOException
	{
		Map<String, PrintWriter> writeMap = Util.treemap();
		
		String pathpatt = Util.sprintf("/userindex/dbslice/%s/part-*", daycode);
		List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), pathpatt);
		FileSystem fsys = FileSystem.get(getConf());
		
		for(Path onepath : pathlist)
		{
			BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, onepath);
			
			for(String line  = bread.readLine(); line != null; line = bread.readLine())
			{
				String pref = line.split("\t")[0];
				String listcode = pref.split(Util.DUMB_SEP)[0];
				
				//Util.pf("\nPrefix is %s, listcode is %s", pref, listcode);
				
				if(!writeMap.containsKey(listcode))
				{
					String listpath = Util.sprintf("/userindex/dbslice/%s/%s", daycode, listcode);
					Util.pf("\nOpening writer for %s, path is %s", listcode, listpath);
					PrintWriter pw = HadoopUtil.getHdfsWriter(fsys, listpath);
					writeMap.put(listcode, pw);
				}
				
				writeMap.get(listcode).write(line + "\n");
			}
			
			// fsys.delete(onepath, true);
		}
		
		for(String listcode : writeMap.keySet())
		{
			writeMap.get(listcode).close();	
		}
	}
}
