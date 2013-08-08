
package com.adnetik.data_management;

import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.Util;
import com.adnetik.shared.Util.ExcName;

// Check to see if the folders and files specified by the Data Management plan 
// are present on HDFS. If not, we might either have to do some deletion,
// or run some additional Synch jobs.
// This is NOT a MapReduce job, it's just a simple Java program that 
// connects to HDFS and uses the Hadoop API
public class DataQuery extends Configured implements Tool
{
	static Set<String> BIG_LOGS = Util.treeset();
	static Set<String> MINI_LOGS = Util.treeset();

	static {
		
		BIG_LOGS.add("no_bid");
		BIG_LOGS.add("bid_all");
		
		MINI_LOGS.add("imp");
		MINI_LOGS.add("click");
		MINI_LOGS.add("conversion");
	}
	
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new DataQuery(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		//checkBigDirectories();
		checkMiniFiles();
		
		System.out.printf("\n\n");
		
		return 0;
	}
	
	void checkMiniFiles() throws IOException
	{
		FileSystem fsys = FileSystem.get(getConf());
		Set<String> miniset = getMiniSet();
		int badFiles = 0;
		int notFoundFiles = 0;
		
		for(String onemini : miniset)
		{
			Path p = new Path(onemini);
			
			if(!fsys.exists(p))
			{
				System.out.printf("\nCould not find file: %s", onemini);
				notFoundFiles++;
			}
		}
		
		for(String logtype : MINI_LOGS)
		{
			Path dataDir = new Path(Util.sprintf("/data/%s/", logtype));
			//System.out.printf("\nData dir is %s", dataDir);
			
			for(FileStatus fstat : fsys.listStatus(dataDir))
			{
				Path sub = fstat.getPath();
				boolean isgood = false;
				
				for(String mini : miniset)
				{
					if(sub.toString().indexOf(mini) > -1)
					{ 
						isgood = true; 
						break;
					}
				}
				
				if(!isgood)
				{
					System.out.printf("\nPath is no good, should be deleted: %s", sub);
					badFiles++;
				}
			}	
		}
		
		System.out.printf("\nFound %d mini files to be deleted, %d files need to be copied", badFiles, notFoundFiles);
	}
	
	Set<String> getMiniSet()
	{
		Set<String> miniset = Util.treeset();
		
		for(String minilog : MINI_LOGS)
		{
			for(String legal : getLegalDayCodes(minilog))
			{
				for(ExcName ename : ExcName.values())
				{
					String fpath = Util.sprintf("/data/%s/%s_%s.lzo", minilog, ename, legal);
					miniset.add(fpath);
				}
			}
		}
			
		return miniset;		
	}
	
	void checkBigDirectories() throws IOException
	{
		// FileSystem	
		FileSystem fsys = FileSystem.get(getConf());

		System.out.printf("\nRunning DataQuery");

		for(String biglog : BIG_LOGS)
		{
			Path dataDir = new Path(Util.sprintf("/data/%s/", biglog));
			Set<String> bscgood = getBasicGood(biglog);
			Set<String> allgood = getGoodDirPaths(biglog);

			for(FileStatus fstat : fsys.listStatus(dataDir))
			{
				Path sub = fstat.getPath();
				boolean isgood = false;
				
				for(String bsc : bscgood)
				{
					if(sub.toString().indexOf(bsc) > -1)
					{
						System.out.printf("\nFound path %s in %s", sub, bsc);	
						isgood = true;
					}
				}
				
				if(!isgood)
				{
					System.out.printf("\nPath is no good, should be deleted: %s", sub);
				}
			}	
		}
	}
	
	Set<String> getBasicGood(String biglog) throws IOException
	{
		Set<String> basicgood = Util.treeset();

		for(String legal : getLegalDayCodes(biglog))
		{
			basicgood.add(Util.sprintf("/data/%s/%s", biglog, legal));
		}
		
		return basicgood;
	}
	
	public Set<String> getLegalDayCodes(String logtype)
	{
		Set<String> legset = Util.treeset();
		Map<String, Integer> logSaveMap = Util.logTypeSaveMap();
		
		Calendar today = new GregorianCalendar();
		Calendar targcal = new GregorianCalendar();		
						
		// old skool date computation
		// Go back in time a given number of days
		int maxDaysBack = logSaveMap.get(logtype);
		
		for(int db = 1; db <= maxDaysBack; db++)
		{
			long targmilli = today.getTimeInMillis();
			targmilli -= db * Util.DAY_MILLI;
			targcal.setTimeInMillis(targmilli);								
			legset.add(TimeUtil.cal2DayCode(targcal));
		}
		
		return legset;
	}
	
	
	// Returns the directories we WANT to have, for today
	public Set<String> getGoodDirPaths(String biglog) throws IOException
	{
		Set<String> goodset = Util.treeset();
		Set<String> bscgood = getBasicGood(biglog);
		
		for(String basic : bscgood)
		{
			for(ExcName ename : ExcName.values())
			{
				String dirpath = Util.sprintf("/%s/%s", basic, ename);
				goodset.add(dirpath);
			}				
		}		
		
		return goodset;
	}
}
