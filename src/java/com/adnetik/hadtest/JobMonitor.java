
package com.adnetik.hadtest;

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

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

public class JobMonitor extends Configured implements Tool
{
	Map<Integer, String> hour2JobMap = Util.treemap();
	SimpleMail logmail = new SimpleMail("Job Monitor Check");
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new JobMonitor(), args);
		System.exit(exitCode);
	}
	
	public JobMonitor()
	{
		super();

		
		String daycode = TimeUtil.getYesterdayCode();
		
		// Starts at 6, should run for at least 2 hours
		for(int hr = 7; hr <= 8; hr++)
			{ hour2JobMap.put(hr, SortScrubCountryUrl.getJobName(daycode)); }		
	}
	
	void messagePf(String format, Object... vargs)
	{
		String m = Util.sprintf(format, vargs);
		Util.pf("%s", m);
		logmail.addLogLine(m.trim());
	}
	
	public int run(String[] args) throws Exception
	{
		boolean dosend = false;		
		int hour = getHourOfDay();
		Set<String> jobset = rJobNames();
		
		messagePf("\nChecking for expected jobs");
		
		for(String onejob : jobset)
		{
			messagePf("\nFound running job: %s", onejob);
		}
		
		// Nothing to check, everything okay
		if(hour2JobMap.containsKey(hour))
		{
			String targjob = hour2JobMap.get(hour);
			
			messagePf("\nExpect to see: %s", targjob);
			
			if(jobset.contains(targjob))
			{
				messagePf("\nFound job %s, as expected");
			} else {
				messagePf("\nWARNING!!! Job %s not found", targjob);	
				dosend = true;
			}
		} else {
			messagePf("No jobs expected at this hour");
		}

		//if(dosend || Math.random() < .2)
		if(dosend)
		{
			logmail.send2admin();	
		}
		
				
		return 1;
	}
	
	@SuppressWarnings( "deprecation" )
	Set<String> rJobNames() throws IOException
	{
		JobConf job = new JobConf(getConf(), this.getClass());
		JobClient jclient = new JobClient(job);
		JobStatus[] jstat = jclient.jobsToComplete();
		
		Set<String> rjobset = Util.treeset();
		
		for(int i = 0; i < jstat.length; i++)
		{
			RunningJob rjob = jclient.getJob(jstat[i].getJobId());			
			//Util.pf("\nJob name is %s", rjob.getJobName());
			rjobset.add(rjob.getJobName());
		}		
		
		return rjobset;
	}
	
	int getHourOfDay()
	{
		Calendar cal = new GregorianCalendar();
		return cal.get(Calendar.HOUR_OF_DAY);
	}
	
}
