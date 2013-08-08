
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
import com.adnetik.shared.Util.*;

import com.adnetik.analytics.*;

// Rename the files named "id" to "improvedigital"
public class RenameID
{		
	public static void main(String[] args) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		
		// List<Path> dirlist = getDirPathList(fsys, LogType.bid_all);
		
		/*
		for(Path onedir : dirlist)
		{
			Util.pf("Found dir %s\n", onedir);	
			
		}
		*/
		
		List<String> daylist = TimeUtil.getDateRange("2012-10-01");
		
		for(String oneday : daylist)
		{
			renameDir(fsys, oneday, LogType.bid_pre_filtered);
			
			
		}
		
		// renameOnePath(fsys, dirlist.get(0));
	}
	
	/*
	private static void renameOnePath(FileSystem fsys, Path onepath) throws IOException
	{
		String pstr = onepath.toString();
		Util.massert(pstr.endsWith("id"));
		
		pstr = pstr.replaceLast("id", "improvedigital");
		
		Path newpath = new Path(pstr);
		
		Util.pf("Going to rename: \n%s\n%s\n", onepath, newpath);
		fsys.rename(onepath, newpath);
		
	}
	*/
	
	private static void renameDir(FileSystem fsys, String daycode, LogType ltype) throws IOException
	{
		Path oldpath = new Path(Util.sprintf("/data/%s/%s/id", ltype, daycode));
		Path newpath = new Path(Util.sprintf("/data/%s/%s/improvedigital", ltype, daycode));
		
		if(fsys.exists(oldpath))
		{
			Util.pf("Going to rename %s-->%s\n", oldpath, newpath);	
			
			fsys.rename(oldpath, newpath);
		}
		
	}
	
	private static List<Path> getDirPathList(FileSystem fsys, LogType ltype) throws IOException
	{
		List<Path> pathlist = Util.vector();
		List<String> daylist = TimeUtil.getDateRange("2012-09-01");
		for(String oneday : daylist)
		{
			String nbpath = Util.sprintf("/data/%s/%s/id", ltype, oneday);
			Path dirpath = new Path(nbpath);
			
			if(fsys.exists(dirpath))
				{ pathlist.add(dirpath); }
		}
		return pathlist;
	}
	
	private static List<Path> getIdPathList(FileSystem fsys, LogType ltype) throws IOException
	{
		String patt = Util.sprintf("/data/%s/id*", ltype.toString());
		
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, patt);
		
		for(Path onepath : pathlist)
		{
			Util.pf("Found path %s\n", onepath);	
			
			
		}
		
		return pathlist;
	}
}
