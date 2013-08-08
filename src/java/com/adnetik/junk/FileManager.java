package com.adnetik.fastetl;

import java.util.*;
import java.io.*;

import com.adnetik.fastetl.FastUtil.MyLogType;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.*;

import java.util.*;

public class FileManager
{
	// TODO: think about NFS write semantics, will the file be completely written??
	
	private Set<String> _cleanSet = Util.treeset();

	FileManager()
	{
		
		
	}
	
	
	void flushCleanList() throws IOException
	{
		// Write to the STAGING (false) path
		FileUtils.writeFileLines(_cleanSet, FastUtil.getCleanListPath(false));
		Util.pf("Wrote %d clean list files\n", _cleanSet.size());
	}
	
	// Grab all the files in the last N days, remove those that have already been processed
	Set<String> newFilesLookBack(String date, int lookback)
	{
		Set<String> newset = FastUtil.getPathsSince(date, lookback);
		newset.removeAll(_cleanSet);
		return newset;
	}
	
	void reportFinished(String filepath)
	{
		Util.massert(!_cleanSet.contains(filepath), "error, file has already been processed %s", filepath);
		_cleanSet.add(filepath);	
	}
	
	void reloadFromSaveDir() throws IOException
	{
		//Util.massert(_bigAgg.get(MyLogType.imp).isEmpty());
		
		// Load clean list
		{
			String cleanpath = FastUtil.getCleanListPath(true);
			List<String> cleanlines = FileUtils.readFileLines(cleanpath);
			_cleanSet.addAll(cleanlines);
			Util.pf("Read %d clean list file\n", _cleanSet.size());
		}
		
		for(String iccpix : new String[] { "icc" , "pixel" })
		{
			File  basedir = new File(Util.sprintf("%s/%s", FastUtil.getBaseDir(true), iccpix));
			// 	AggregationEngine.cookie_agg.loadFromBaseDir(basedir);
		}
		
		// load general aggregators from file
		//cheesy way to get icc and pixel directories
		Set<String> aggpathset = FastUtil.getAggPathSet(true);
		
		MyLogType[] lt = new MyLogType[] { MyLogType.pixel, MyLogType.imp };
		for(MyLogType mtype: lt)
		{
			// List<String> filepaths = FastUtil.getQuatPaths(mtype);
			// AggregationEngine.loadFromFiles(filepaths);
			
			//File  basedir = new File(Util.sprintf("%s/%s", FastUtil.getQuatPath(mtype, relid));
		}
	}	
}
