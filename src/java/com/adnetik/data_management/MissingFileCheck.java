
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
import com.adnetik.shared.Util.*;

// Hadoop-connected, but not MapReduce job.
public class MissingFileCheck extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new MissingFileCheck(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// FileSystem	
		FileSystem fsys = FileSystem.get(getConf());

		Map<String, Integer> logSaveMap = Util.logTypeSaveMap();
				
		//for(LogType ltype : new LogType[] { LogType.imp })
		for(LogType ltype : LogType.values())
		{
			if(Util.isBig(ltype))
				{ continue; }
			
			// Say cleanup is running in AM of 09/19, and we are going to save 5 days.
			// Then we save 09/18, 09/17, 09/16, 09/15, 09/14, 
			// and we delete 09/13=(09/19-(5+1))
			int numDaySave = logSaveMap.get(ltype.toString());
			
			numDaySave = (numDaySave > 75 ? 75 : numDaySave);
			
			for(String targCode : getDayList(numDaySave))
			{
				for(ExcName onex : ExcName.values())
				{
					Path datapath = HadoopUtil.findHdfsLzoPath(fsys, onex, ltype, targCode);
					
					if(datapath == null)
					{
						System.out.printf("\n%s\t%s\t%s", onex, ltype, targCode);
					}
				}				
			}

		}
			
		// Pixel Log Files also
		{	
			int numDaySave = logSaveMap.get("pixel");

			for(String targCode : getDayList(numDaySave))
			{
				Path dataPath = new Path(HadoopUtil.getHdfsLzoPixelPath(targCode));
				
				if(!fsys.exists(dataPath))
				{
					System.out.printf("\npixel\t%s", targCode);
				}				
			}
		}		
		
		return 0;
	}	
	
	static List<String> getDayList(int numDaySave)
	{
		Calendar today = new GregorianCalendar();
		Calendar targcal = new GregorianCalendar();

		// old skool date computation
		// Go back in time a given number of days
		long todaymilli = today.getTimeInMillis();

		List<String> daylist = Util.vector();
		
		// Actually want to start TWO days back
		// Files from yesterday might not be missing, they might be just not yet copied
		for(int nd = 2; nd < numDaySave; nd++)
		{
			long targmilli = todaymilli - nd*Util.DAY_MILLI;
			targcal.setTimeInMillis(targmilli);	
			daylist.add(TimeUtil.cal2DayCode(targcal));
		}
		
		return daylist;
	}
}
