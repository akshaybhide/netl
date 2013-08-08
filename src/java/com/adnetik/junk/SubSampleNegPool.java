
package com.adnetik.data_management;

import java.io.*;
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

import com.adnetik.userindex.*;


public class SubSampleNegPool extends Configured implements Tool
{
	SimpleMail logMail;
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());		

		{
			Text a = new Text("");			
			HadoopUtil.alignJobConf(job, new SubSampleMapper(), new SubSampleReducer(), a, a, a, a);
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		String logfile = (args.length >= 2 ? args[1] : null);
		
		logMail = new SimpleMail(Util.sprintf("SubSampleNegPool %s", daycode));
		
		FileInputFormat.setInputPaths(job, HadoopUtil.getUserSetPath(daycode));

		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		
		// Output path
		Path outputFilePath = new Path(Util.sprintf("/userindex/negpools/pool_%s", daycode));
		fSystem.delete(outputFilePath, true);		
		logMail.pf("\nUsing dir %s\n", outputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);		

		job.setJobName(Util.sprintf("SubSampleNegPool %s", daycode));
		job.setNumReduceTasks(10);
		
		// Submit the job, then poll for progress until the job is complete
		boolean success = false;
		try 
		{
			RunningJob jobrun = JobClient.runJob(job);		
			success = jobrun.isSuccessful();
		} catch (Exception ex) {
			success = false;
			ex.printStackTrace();
		}		
		
		// Okay, now that we're done, compile the Negative Pool Info into the staging area
		compileNegativeInfo(fSystem, daycode);

		logMail.logOrSend(logfile);

		return 0;
	}
		
	// Compiles a negative user index pool
	public void compileNegativeInfo(FileSystem fsys, String daycode) throws IOException
	{
		// /userindex/negpools/pool_2012-02-08/part-00002
		String patt = "/userindex/negpools/pool_*/part-*";
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, patt);
		
		logMail.pf("\nFound %d neg-pool paths", pathlist.size());
		
		int lcount = 0;
		double startup = Util.curtime();
		
		// TODO: this should be a daycode-specific path.
		OutputStream fos = fsys.create(new Path(UserIndexUtil.getStagingInfoPath(UserIndexUtil.StagingType.negative, daycode)));
		Set<String> daycodeset = Util.treeset();
		
		for(Path p : pathlist) 
		{
			//Util.pf("Reading from path %s\n", p);
			BufferedReader reader = HadoopUtil.hdfsBufReader(fsys, p);
			
			daycodeset.add(Util.findDayCode(p.toString()));
			
			for(String s = reader.readLine(); s != null; s = reader.readLine())
			{
				String[] ccode_wtp = s.split("\t");
				
				// TODO: this seems brittle
				String pixcode = Util.sprintf("negative_%s_000", ccode_wtp[0]);
				String writeline = Util.sprintf("%s\t%s\n", ccode_wtp[1], pixcode);
				
				// TODO: this is probably inefficient, there should be a Writer command or something
				fos.write(writeline.getBytes());
				lcount++;
			}
		}
		
		fos.close();
		
		double endtime = Util.curtime();
		logMail.pf("\nCompile Negative: wrote %d lines in %.03f seconds \n\tfrom daycodeset %s", lcount, (endtime-startup)/1000, daycodeset);
	}			
	
	
	public static class SubSampleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		String dumbStringLookup;
		
		@Override 
		public void configure(JobConf job) {
			dumbStringLookup = UserIndexUtil.getListenCodeMap(job).toString();
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			String[] toks = value.toString().split("\t");
			
			if(toks.length < 2)
				{ return; }
			
			String ccode = toks[1];
			String lookup = Util.sprintf("negative_%s", ccode);
			
			if(dumbStringLookup.indexOf(lookup) > -1)
			{
				output.collect(new Text(ccode), new Text(toks[0]));
			}
		}
	}
	
	public static class SubSampleReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		
		public static class MiniPack
		{
			SortedMap<Long, String> popmap = Util.treemap();
			Set<String> popset = Util.treeset();
			
			Random jRand = new Random();
			
			int cap; 
			
			public MiniPack(int c)
			{
				cap = c;
			}
			
			public void addMaybe(String s)
			{
				if(!popset.contains(s))
				{
					long id = jRand.nextLong();
					popmap.put(id, s);
					popset.add(s);
				}
				
				winnow();
			}
			
			void winnow()
			{
				while(popmap.size() > cap)
				{ 
					long lastkey = popmap.lastKey();
					String lastval = popmap.get(lastkey);
					
					popmap.remove(lastkey);
					popset.remove(lastval);
				}				
			}
			
			Set<String> values()
			{
				return popset;
			}
		}
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			// TODO: set this intelligently
			MiniPack mpack = new MiniPack(5000);
			
			while(values.hasNext())
			{
				mpack.addMaybe(values.next().toString());
			}
			
			for(String val : mpack.values())
			{
				collector.collect(key, new Text(val));
			}
		}

	}			
	
}
