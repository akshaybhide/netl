/**
 * 
 */
package com.adnetik.slicerep;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.*;

import com.adnetik.bm_etl.BmUtil.*;


public class HadoopMode extends Configured implements Tool 
{	
	public static void main(String[] args) throws Exception
	{ 
		HadoopUtil.runEnclosingClass(args);
	}
	  
	public int run(String[] args) throws Exception
	{
		// This option tells Hadoop to reuse the JVMs
		getConf().setInt("mapred.job.reuse.jvm.num.tasks", 10);
		
		// getConf().setInt("mapred.user.jobconf.limit", 10000000);
		
		//  This is only in for the 2012-11-22 job, which has corrupt 
		// log files in it
		// getConf().setInt("mapred.max.map.failures.percent", 1);

		// getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);		
		
		String daycode = args[0];
		daycode = ("yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode);
		if(!TimeUtil.checkDayCode(daycode))
		{ 
			Util.pf("\nBad day code : %s" , daycode);
			System.exit(1);
		}

		ArgMap argmap = Util.getClArgMap(args);
		Map<String, String> optargs = Util.getClArgMap(args);
		boolean usebid = argmap.getBoolean("usebid", true);
		Integer maxfile = argmap.getInt("maxfile", Integer.MAX_VALUE);
		
		// This is not strictly necessary; do it for failfast
		CatalogUtil.initSing(daycode, DbTarget.internal); 

		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Set various strings related to job
		{
			// SortedSet<DimCode> dimset = DatabaseBridge.getDimSet(getAggType(optargs), isinternal);
			// Util.pf("\nDimension set is %s", dimset);
			job.setStrings("EXPAND_DATE", "false");	
			job.setStrings("DAY_CODE", daycode);
			// dimSetMap = SliDatabase.getDimSetMap();
		}
		
		// Align Job
		{
			Text a = new Text("");	
			Metrics mtcs = new Metrics();
			HadoopUtil.alignJobConf(job, false, new SliMapper(), new AggReducer(), null, a, mtcs, a, a);		
		}
		
		Util.pf("Attempting to look up dimension map...");
		Map<AggType, SortedSet<DimCode>> dimSetMap = DatabaseBridge.getDimSetMap(DbTarget.internal);
		Util.pf(" ... done, map is %s\n", dimSetMap);
		
		// With this and the mixed-input flag set above, we can deal with anything. Maybe
		// job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		
		// Deal with input files 
		// usehdfs=false
		List<Path> pathlist = SliUtil.getPathList(daycode, maxfile, false, usebid);
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
		
		// Deal with output path
		{
			Path outputPath = new Path(BmUtil.getOutputPath(daycode, DbTarget.internal));
			Util.pf("\nTarget Output path is %s", outputPath);
			HadoopUtil.moveToTrash(this, outputPath);
			// FileSystem.get(getConf()).delete(outputPath, true);
			//HadoopUtil.checkRemovePath(FileSystem.get(getConf()), outputPath);
			FileOutputFormat.setOutputPath(job, outputPath);	
		}
		
		job.setJobName("SliceRepHadoop " + daycode);
		job.setNumReduceTasks(HadoopUtil.BIG_JOB_POLITE_NODES); 	 	
			
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
	
		return 1;
	}
	
	public static class AggReducer extends MapReduceBase implements Reducer<Text, Metrics, Text, Text> 
	{
		@Override
		public void reduce(Text key, Iterator<Metrics> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{		
			Metrics result = new Metrics();
			
			while(values.hasNext())
			{
				result.add(values.next());	
			}
			
			collector.collect(key, new Text(result.toString("&")));
		}
	}	
	
}
