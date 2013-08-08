
package com.adnetik.data_management;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;
import com.adnetik.analytics.InterestUserUpdate;

public class CronJobCentral
{
	private static double sttTime;
	private static double endTime;
	
	
	public static void main(String[] args) throws Exception
	{
		CronJobCentral cjc = new CronJobCentral();
		SimpleMail logmail = new SimpleMail("CronJobReport");

		// This doesn't REALLY need special treatment, because it should just default to force=true
		// But I want to tread cautiously
		{
			String[] sdestroyArgs = new String[] { "force=true" };
			SearchDestroy sdest = new SearchDestroy();
			cjc.runTheTool(sdest, sdestroyArgs);
			
			// Report the directories that we actually deleted
			for(Path delpath : sdest.delList)
			{
				logmail.addLogLine(Util.sprintf("Deleted directory %s", delpath.toString()));	
			}
		}		
		logmail.addLogLine("Finished deleting old files");
		
		runTheTool(new DailyNegativeUserScan(), args);
		logmail.addLogLine("Finished DailyNegativeUser Scan");

		runTheTool(new LogInterestActivity(), args);
		logmail.addLogLine("Finished LogInterestActivity");

		
		{
			// Don't want to run secondary slice if primary failed.
			boolean mainsucc = runTheTool(new SliceInterestActivity(), args);
			logmail.addLogLine("Finished SliceInterestActivity, success=" + mainsucc);

			/*
			This doesn't work for obscure reasons relating to how the jobconf is setup.
			if(mainsucc)
			{ 
				runTheTool(new SliceInterestSecond(), args); 
				logmail.addLogLine("Finished SliceInterestSecond");
			}
			*/
		}
		
		// This now goes separately, at 8PM.
		//runTheTool(new SortScrub(), args);
		
		logmail.send2admin();
		Util.pf("\n\n");
	}
	
	private static boolean runTheTool(Tool torun, String[] args) throws Exception
	{
		boolean success = true;
		String className = torun.getClass().getSimpleName();
		Util.pf("\nJob %s running at %s", className, new Date());
		
		timeS();
		try { int retcode = ToolRunner.run(torun, args); } 
		catch (Exception ex) { success = false; }
		timeE();
		
		
		printTimeData(className);
		return success;
	}
	
	static void printTimeData(String jobName)
	{
		Util.pf("\nFinished job %s \n\tat %s \n\ttook %.03f seconds",
			jobName, new Date(), (endTime-sttTime)/1000);
					
	}
	
	private static void timeS()
	{
		sttTime = System.currentTimeMillis();	
	}
	
	private static void timeE()
	{
		endTime = System.currentTimeMillis();	
	}
	
}
