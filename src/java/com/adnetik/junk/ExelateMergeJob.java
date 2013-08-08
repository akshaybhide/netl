
package com.adnetik.data_management;

import java.io.*;
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
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

public class ExelateMergeJob extends Configured implements Tool
{
	public static final long MAX_HEX_PREF = Long.valueOf("ffffffff", 16);

	private static TreeMap<String, Integer> _PART_MAP;
	
	private static final Path PROC_PATH = new Path("/userindex/exelate/PROC_DAYS.txt");
	
	private static final Path SHARD_PATH = new Path("/userindex/exelate/shards");
	private static final Path GIMP_PATH = new Path("/userindex/exelate/gimp");
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args); 
	}
	
	public int run(String[] args) throws Exception
	{			
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		// align
		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new ExMergeMapper(), new ExMergeReducer(), a, a, a, a);
		}
		
		Map<String, Integer> partmap = getPartMap(20);
		
		for(String s : partmap.keySet())
		{
			Util.pf("\nPart %s --> %d", s, partmap.get(s));	
			
		}
		
		//System.exit(1);
		
		SortedSet<Integer> proc_set = getProcSet();
		SortedSet<Integer> targ_set = getTargSet(proc_set);
		
		while(targ_set.size() > 10)
		{
			targ_set.remove(targ_set.last());	
			
		}
		
		// Rename path
		fSystem.rename(SHARD_PATH, GIMP_PATH);
		
		Util.pf("\nTarg set is %s", targ_set);
		
		// File Inputs - either the daily LZO file, if it exists, or the list of GZip files on NFS.
		{
			//Gotcha - now the input files are in a different directory
			List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), GIMP_PATH + "/part*");

			for(Integer oneday : targ_set)
			{
				String onepath = Util.sprintf("/userindex/exelate/datadumps/exelate_%d.tsv", oneday);
				pathlist.add(new Path(onepath));
			}
			
			
			Util.pf("\nFound pathlist = %s", pathlist);
			//logMail.pf("\nFound %d days worth of pixel data", pathlist.size());
			// job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);				
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
		//System.exit(1);
		
		job.setNumReduceTasks(20); // defaults to 1
		job.setPartitionerClass(WtpIdPartitioner.class);
		
		HadoopUtil.checkRemovePath(fSystem, SHARD_PATH);
		//logMail.pf("\nUsing temp dir %s", tempDirPath);
		FileOutputFormat.setOutputPath(job, SHARD_PATH);		
		
		Util.pf("\nCalling ExelateMergeJob");
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("ExelateMergeJob"));
		

		RunningJob jobrun = JobClient.runJob(job);		
		boolean success = jobrun.isSuccessful();
			
		// Write newly updated proc file.
		{
			Util.pf("\nWriting targ set %s", targ_set);
			PrintWriter pwrite = HadoopUtil.getHdfsWriter(fSystem, PROC_PATH);	
			for(Integer onetarg : targ_set)
				{ pwrite.write(onetarg + "\n"); }	

			for(Integer onetarg : proc_set)
				{ pwrite.write(onetarg + "\n"); }	

			pwrite.close();
		}
		
		// Delete GIMP path
		{
			Util.pf("\nDeleting gimp path...");
			fSystem.delete(GIMP_PATH, true);
			Util.pf(" ... done");
		}
		
		return 0; 
	}

	SortedSet<Integer> getProcSet() throws IOException
	{
		SortedSet<Integer> procset = Util.treeset();
		
		FileSystem fsys = FileSystem.get(getConf());
		if(!fsys.exists(PROC_PATH))
			{ return procset; }
		
		for(String oneline : HadoopUtil.readFileLinesE(getConf(), PROC_PATH))
		{
			String dc = oneline.trim();
			
			if(dc.length() == 0)
				{ continue; }

			procset.add(Integer.valueOf(dc));			
		}
		
		return procset;
	}
	
	SortedSet<Integer> getTargSet(Set<Integer> procset) throws IOException
	{
		SortedSet<Integer> targset = Util.treeset();
		String pathpatt = "/userindex/exelate/datadumps/exelate*.tsv";
		
		List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), pathpatt);
		
		String extag = "exelate_";
		
		for(Path onepath : pathlist)
		{
			String pathstr = onepath.toString();
			int start = pathstr.indexOf(extag)+extag.length();
			
			Integer daycode = Integer.valueOf(pathstr.substring(start, start+8));
			
			if(daycode < 20120125)
				{ continue; }
			
			if(!procset.contains(daycode))
			{ 
				targset.add(daycode); 
			} else {
				
				Util.pf("\nFound %d in procset, skipping", daycode);	
			}
 		}
		
		return targset;
	}	
	
	
	public static class ExMergeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			String record = value.toString();
			
			if(record.indexOf("COUNTRY") > -1)
				{ return; }
			
			try {
				String[] toks = record.split("\t");
			
				// TODO: check if the WTP ID is legit
				String val = Util.sprintf("%s\t%s\t%s", toks[0], toks[1], toks[3]);
				
				// Util.pf("Found %d tokens, line is: \n\t%s", toks.length, record);
				
				output.collect(new Text(toks[2]), new Text(val));	
				
				
			} catch (Exception ex) {
				
				//throw new RuntimeException(ex);
				reporter.incrCounter(HadoopUtil.Counter.GenericError, 1);				
			}
		}
	}
	
	public static class ExMergeReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			try {
				long most_recent = -1;
				String segments = null;
				String country = null;
				
				while(values.hasNext())
				{
					// timestamp, country, segment info
					String[] toks = values.next().toString().split("\t");
					
					long newts = Long.valueOf(toks[0]);
					
					if(most_recent < newts)
					{	
						most_recent = newts;
						country = toks[1];
						segments = toks[2];
					}
				}
				
				String val = Util.sprintf("%s\t%s\t%s", country, key.toString(), segments);
				collector.collect(new Text(""+most_recent), new Text(val));

				reporter.incrCounter(HadoopUtil.Counter.ProcUsers, 1);

			} catch (Exception ex) {
				
				throw new RuntimeException(ex);
				
				//reporter.incrCounter(HadoopUtil.Counter.ReducerExceptions, 1);
			}
		}		
	}	
	
	// Key point: all of the info related to a given pixel id should be sent
	// to the same reducer
	public static class WtpIdPartitioner implements Partitioner<Text, Text>
	{
		public void configure(JobConf jobconf) {
			//super.configure(jobconf);
		}
		
		public int getPartition(Text key, Text value, int numPart)
		{
			String wtpid = key.toString();
			return calcPartition(wtpid, numPart);
		}
	}	
	
	public static int calcPartition(String wtpid, int numpart)
	{
		int dashpos = wtpid.indexOf("-");
		String idpref = wtpid.substring(0, dashpos);		
		TreeMap<String, Integer> pmap = getPartMap(numpart);
		return pmap.get(pmap.ceilingKey(idpref))-1; // Wow, how did this work before????
	}
	
	public static TreeMap<String, Integer> getPartMap(int numpart)
	{
		if(_PART_MAP == null)
		{
			_PART_MAP = Util.treemap();
			
			long delta = MAX_HEX_PREF / numpart;
			
			for(int i = 1; i < numpart; i++)
			{
				String cutoff = Long.toHexString(i*delta);
				
				while(cutoff.length() < 8)
					{ cutoff = "0" + cutoff; }
				
				_PART_MAP.put(cutoff, i);
			}
			
			// This is higher than all valid WTP IDs
			_PART_MAP.put("g", numpart);				
		} 
		
		Util.massert(_PART_MAP.size() == numpart, "Error: must call with same number of partitions");
	
		return _PART_MAP;
	}
}
