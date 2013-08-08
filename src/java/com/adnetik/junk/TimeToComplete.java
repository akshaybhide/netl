
package com.adnetik.analytics;

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

public class TimeToComplete extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new TimeToComplete(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{				
		Map<String, String> optArgs = Util.treemap();
		Util.putClArgs(args, optArgs);

		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());		

		// Set the input paths, either a manifest file or a glob path
		Set<Path> pathset = Util.treeset();
		pathset.addAll(HadoopUtil.pathListFromClArg(getConf(), args[0]));
		AbstractScanJob.addClPathInfo(pathset, getConf(), optArgs);
		
		Util.pf("\nFound %d total input paths", pathset.size());
		
		long totLen = 0;
		
		for(Path p : pathset)
		{
			FileSystem pathSys = p.getFileSystem(job);
			long plen = pathSys.getFileStatus(p).getLen();
			totLen += plen;
			//Util.pf("\nLength of %s = %d", p, plen);
		}
		
		Util.pf("\nTotal size of %d input paths is %d\n\n", pathset.size(), totLen);
		
		return 0;
	}
}

