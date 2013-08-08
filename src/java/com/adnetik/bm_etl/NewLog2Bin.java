/**
 * 
 */
package com.adnetik.bm_etl;

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

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


public class NewLog2Bin extends Configured implements Tool 
{	
	public static void main(String[] args) throws Exception
	{ 
		HadoopUtil.runEnclosingClass(args);
	}
	  
	public int run(String[] args) throws Exception
	{
		// This option tells Hadoop to reuse the JVMs
		getConf().setInt("mapred.job.reuse.jvm.num.tasks", 10);
		
		// getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);		
		
		String daycode = args[0];
		daycode = ("yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode);
		if(!TimeUtil.checkDayCode(daycode))
		{ 
			Util.pf("\nBad day code : %s" , daycode);
			System.exit(1);
		}

		Util.printStartFlagInfo(EtlJob.NewLog2Bin, daycode);
		Map<String, String> optargs = Util.getClArgMap(args);
		DbTarget dbtarg = DbTarget.external;
		boolean usebid = "true".equals(optargs.get("usebid"));
		Integer maxFileCount = optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : null;
		
		// This is not strictly necessary; do it for failfast
		CatalogUtil.initSing(daycode, DbTarget.external); 

		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Set various strings related to job
		{
			// TODO: this job is no longer agg-specific
			SortedSet<DimCode> dimset = DatabaseBridge.getDimSet(getAggType(optargs), dbtarg);
			Util.pf("\nDimension set is %s", dimset);
			job.setStrings("DIM_SET", dimset.toString());
			job.setStrings("EXPAND_DATE", "false");	
			job.setStrings("DAY_CODE", daycode);
		}
		
		// Align Job
		{
			Text a = new Text("");	
			Metrics mtcs = new Metrics();
			HadoopUtil.alignJobConf(job, false, new MapTransformer(), new AggReducer(), new CampaignPartitioner(), a, mtcs, a, a);		
		}
		
		// With this and the mixed-input flag set above, we can deal with anything. Maybe
		// job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		
		// Deal with input files
		List<Path> pathlist = getPathList(daycode, maxFileCount, true, usebid);
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
		
		// Deal with output path
		{
			Path outputPath = new Path(BmUtil.getOutputPath(daycode, dbtarg));
			Util.pf("\nTarget Output path is %s", outputPath);
			HadoopUtil.moveToTrash(this, outputPath);
			// FileSystem.get(getConf()).delete(outputPath, true);
			//HadoopUtil.checkRemovePath(FileSystem.get(getConf()), outputPath);
			FileOutputFormat.setOutputPath(job, outputPath);	
		}
		
		job.setJobName(getJobName(daycode, getAggType(optargs)));
		job.setNumReduceTasks(HadoopUtil.BIG_JOB_POLITE_NODES); 	 	
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		Util.printEndFlagInfo(EtlJob.NewLog2Bin, daycode);
		return 1;
	}
	
	public static String getJobName(String dcode, AggType atype)
	{
		return "NewLog2Bin " + atype + ":" + dcode; 
	}
	
	private AggType getAggType(Map<String, String> optargs)
	{
		String typestr = optargs.get("aggtype");
		return (typestr == null ? AggType.ad_general : AggType.valueOf(typestr));		
	}
	
	List<Path> getPathList(String daycode, Integer maxcount, boolean usehdfs, boolean usebid) throws IOException
	{
		LogType[] touse;
		{
			if(usebid)
				touse = new LogType[] { LogType.bid_all, LogType.imp, LogType.click, LogType.conversion };
			else
				touse = new LogType[] { LogType.imp, LogType.click, LogType.conversion };
		}
		
		
		List<Path> subblist = Util.vector();
		FileSystem fsys = FileSystem.get(getConf());
		
		for(LogType ltype : touse)
		{
			for(ExcName exc : ExcName.values())
			{
				List<String> exlist = Util.getNfsLogPaths(exc, ltype, daycode);
				
				if(exlist == null)
					{ continue; }
				
				for(String onepath : exlist)
					{ subblist.add(new Path("file://" + onepath)); }
			}
		}	
		
		// TODO: make this an option
		Collections.shuffle(subblist);				
		
		List<Path> pathlist = Util.vector();
		
		for(Path p : subblist)
		{
			pathlist.add(p);
			
			if(maxcount != null && pathlist.size() >= maxcount)
				{ break; }
		}
		
		// Util.pf("\nPath list is %s", pathlist);
		
		return pathlist;
	}
	
	public static class CampaignPartitioner implements Partitioner<Text, Metrics>
	{
		public void configure(JobConf jobconf) {}
		
		public int getPartition(Text key, Metrics value, int numPart)
		{
			Map<String, String> parsemap = BmUtil.getParseMap(key.toString());
			String campid = parsemap.get(DimCode.campaign.toString());
			
			// Want to log this or something?
			try { return (Integer.valueOf(campid) % numPart); }
			catch (Exception ex) { return 0; }
		}
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
