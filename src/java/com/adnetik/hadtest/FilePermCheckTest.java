
package com.adnetik.hadtest;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;


public class FilePermCheckTest extends Configured implements Tool
{
	public static final String EMPTY_LZO_PATH = "/mnt/src/java/data_management/emptyfile.lzo";
	
	public static final Long SIZE_PER_REDUCER = 5000000000L;
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());		

		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new HadoopUtil.EmptyMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);
		}
					
		// Specify various job-specific parameters     

		ExcName excname = ExcName.admeld;
		LogType logtype = LogType.click;
		String  daycode = "2012-02-15";
		
		job.setJobName(Util.sprintf("FilePermTest %s %s %s", excname, logtype, daycode));
		
		List<Path> pathlist = Util.vector();
		{
			String nfsdir = Util.getNfsDirPath(excname, logtype, daycode);
			String pathpattern = Util.sprintf("file://%s*.log.gz", nfsdir);
			
			for(Path onep : HadoopUtil.getGlobPathList(getConf(), pathpattern))
			{
				if(pathlist.size() < 10)
					{ pathlist.add(onep); }
			}
			
			//pathlist.addAll(HadoopUtil.getGlobPathList(getConf(), pathpattern));
			Util.pf("\nFound %d paths of form %s\n", pathlist.size(), pathpattern);
		}
		
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));	

		{
			Path onlyHdfsPath = new Path("/data/imp/filepermtest");
			//HadoopUtil.checkWritePerm(fSystem, onlyHdfsPath);
			
			// With this line, the job should run for a while, then fail.
			FileOutputFormat.setOutputPath(job, onlyHdfsPath);		
		}
		
		// Submit the job, then poll for progress until the job is complete
		RunningJob runJob = JobClient.runJob(job);		
		
		
		return 0;
	}
		
}
