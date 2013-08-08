
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

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;


// This is NOT a MapReduce job, it's just a simple Java program that 
// connects to HDFS and uses the Hadoop API
public class HdfsCleanup // extends Configured implements Tool
{
	/*
	List<String> paths = Util.vector();
	Set<Integer> failIndex = Util.treeset();
	
	// Deletion targets that were not found
	int notFound = 0;
	
	// Count of successfully deleted paths
	int delCount = 0;
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new HdfsCleanup(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// FileSystem	
		FileSystem fsys = FileSystem.get(getConf());

		Path dataDir = new Path("/data/");
		
		for(FileStatus fstat : fsys.listStatus(dataDir))
		{
			Path sub = fstat.getPath();
			//System.out.printf("\nPath is %s", sub);
		}
		
		Map<String, Integer> logSaveMap = Util.logTypeSaveMap();
		
		Calendar today = new GregorianCalendar();
		Calendar targcal = new GregorianCalendar();
		
		for(String logtype : logSaveMap.keySet())
		{
			// old skool date computation
			// Go back in time a given number of days
			long targmilli = today.getTimeInMillis();
			
			// Say cleanup is running in AM of 09/19, and we are going to save 5 days.
			// Then we save 09/18, 09/17, 09/16, 09/15, 09/14, 
			// and we delete 09/13=(09/19-(5+1))
			targmilli -= (logSaveMap.get(logtype)+1) * Util.DAY_MILLI;
			targcal.setTimeInMillis(targmilli);				
			
			String targCode = TimeUtil.cal2DayCode(targcal);
			
			System.out.printf("\nTarget for logtype=%s code is %s", logtype, targCode);
			
			if(DataQuery.BIG_LOGS.contains(logtype.toString()))
			{
				String targDirPath = Util.sprintf("/data/%s/%s/", logtype, targCode);
				System.out.printf("\nTargeting directory for deletion: %s", targDirPath);
				paths.add(targDirPath);				
			
			} else {
	
				for(ExcName onex : ExcName.values())
				{		
					String lzoPath = HadoopUtil.getHdfsLzoPath(onex, BidLogEntry.lookupLog(logtype), targCode);
					paths.add(lzoPath);						
				}
			}
		}
		
		for(int i = 0; i < paths.size(); i++)
		{
			Path onepath = new Path(paths.get(i));
			
			if(!fsys.exists(onepath))
			{
				notFound++;	
				continue;
			}
			
			boolean del = fsys.delete(onepath, true);
			
			if(!del)
				{ failIndex.add(i); }
			else 
				{ delCount++; }
		}
		
		System.out.printf("\nCleanup finished");
		System.out.printf("\n\tdeleted %d paths", delCount);
		System.out.printf("\n\tnot found %d paths", notFound);
		
		if(failIndex.size() > 0)
		{
			System.out.printf("\n\tFAILED to DELETE %d paths", failIndex.size());
		
			for(int fail : failIndex)
			{
				System.out.printf("\nFailed to delete %s", paths.get(fail));
			}
		}
		
		System.out.printf("\n\n");
		
		return 0;
	}
	*/
}
