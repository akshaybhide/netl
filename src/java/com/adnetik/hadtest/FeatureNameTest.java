
package com.adnetik.hadtest;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

public class FeatureNameTest extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{		
		// Make sure we are using both .txt and .lzo
		getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);				
		
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());		
		String partcode = "0";
		partcode = Util.padLeadingZeros(Integer.valueOf(partcode), 5); 
		String canonday = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
		
		{
			// This is just to check that everything is configured correctly,
			// before running the job.
			Map<String, AdaBoost<UserPack>> pix2class = UserIndexUtil.readClassifData(fSystem);
			Util.pf("\nFound classifiers for pixel ids: %s", pix2class.keySet());
			
			for(AdaBoost<UserPack> ada : pix2class.values())
			{
				for(String onefunc : ada.getRealFuncNames())
				{
					Util.massert(StrayerFeat.getFeatMap().containsKey(onefunc),
						"Function name %s not found in feat map", onefunc);
				}
			}
			
			Util.pf("All features found in function map\n");
			// System.exit(1);
		}
		
		{
			String sysprop = System.getProperty("file.encoding");
			String propstr = Util.sprintf("fencoding___%s", sysprop);
			Util.pf("Local string is %s\n", propstr);
		}
		
		{
			String charprop = java.nio.charset.Charset.defaultCharset().displayName();
			String charstr = Util.sprintf("defchar___%s", charprop);
			Util.pf("Local string is %s\n", charstr);
		}
		
		{
			int find = 0;
			
			for(String onefunc : StrayerFeat.getFeatMap().keySet())
			{
				FeatureCode fcode = StrayerFeat.getFeatMap().get(onefunc).getCode();
				if(fcode != FeatureCode.vertical)
					{ continue; }
				// if(!StrayerFeat.getFeatMap().get(onefunc)
				
				String idkey = Util.sprintf("fid___%s", Util.padLeadingZeros(find, 5));
				int hashcode = onefunc.hashCode();
				//Set<Integer> idset = Util.treeset();
				//idset.add(hashcode);
				Util.pf("%s\t%s\n", idkey, hashcode);
				find++;
			}
			// System.exit(1);
		}
		
		// System.exit(1);
		
					
		{
			// TODO: replace with dayRange or something
			// List<String> daylist = TimeUtil.getDateRange(1);
			
			List<String> daylist = UserIndexUtil.getCanonicalDayList();
			
			List<Path> pathlist = Util.vector();
			for(String daycode : daylist)
			{
				String spath = Util.sprintf("/userindex/sortscrub/%s/part-%s.*", daycode, partcode);
				pathlist.add(new Path(spath));
				break; // just need one
			}
						
			Util.pf("\nFound %d paths", pathlist.size());
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));
			job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		}
					
		{
			Text a = new Text("");
			// HadoopUtil.alignJobConf(job, new EncodingTestMapper(), new ScoreUserJob.ScoreReducer(), a, a, a, a);
			HadoopUtil.alignJobConf(job, new EncodingTestMapper(), new HadoopUtil.SetReducer(), a, a, a, a);
			
		}
		
		{
			String outputPath = Util.sprintf("/userindex/testencoding", canonday, partcode);
			HadoopUtil.moveToTrash(this, new Path(outputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		}
			
		// Specify various job-specific parameters 
		job.setJobName(Util.sprintf("TestEncoding PartCode=%s CanDay=%s", partcode, canonday));
		
		job.setPartitionerClass(ScoreUserJob.Reshuffler.class);
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	

		return 1;

	}

	// TODO: put all of these mappers and reducers in their own file
	public static class EncodingTestMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		// Pixel ID --> classifier
		Map<String, AdaBoost<UserPack>> classifMap;
			
		Map<String, boolean[]> funcMap = Util.treemap();
		
		Random sanRand = new Random();		
		
		Integer myCount = 0;
		
		@Override
		public void configure(JobConf job)
		{
			Util.pf("\nCONFIGURING -----------------------");
			
			try { 
				FileSystem fsys = FileSystem.get(job);

				classifMap = UserIndexUtil.readClassifData(fsys);
				
				//Util.pf("\nFound pixel set %
				
			} catch (Exception ex) {
				
				Util.pf("\nError configuring filter");
				throw new RuntimeException(ex);
			}
		}
		
		private void initFuncMap()
		{
			for(String listcode : classifMap.keySet())
			{
				for(String onefunc : classifMap.get(listcode).getRealFuncNames())
				{
					// evaluating one user at a time, so only really need one-element array
										
					Util.massert(StrayerFeat.getFeatMap().containsKey(onefunc),
						"Function name %s not found in feat map", onefunc);
					funcMap.put(onefunc, new boolean[1]);	
				}
			}
		}
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{		
			
			if(funcMap.size() == 0)
			{
				// initFuncMap();
			}
			
			if(myCount < 10)
			{
				/*
				{
					String sysprop = System.getProperty("file.encoding");
					String propstr = Util.sprintf("fencoding___%s", sysprop);
					output.collect(new Text(propstr), new Text("1"));
				}
				
				{
					String charprop = java.nio.charset.Charset.defaultCharset().displayName();
					String charstr = Util.sprintf("defchar___%s", charprop);
					output.collect(new Text(charstr), new Text("1"));
				}
				*/
				
				int find = 0;
				
				for(String onefunc : StrayerFeat.getFeatMap().keySet())
				{
					FeatureCode fcode = StrayerFeat.getFeatMap().get(onefunc).getCode();
					if(fcode != FeatureCode.vertical)
						{ continue; }					
					
					String idkey = Util.sprintf("fid___%s", Util.padLeadingZeros(find, 5));
					int hashcode = onefunc.hashCode();
					output.collect(new Text(idkey), new Text("" + hashcode));
					find++;
				}
				
				myCount++;
				
			}
		}
	}		
}

