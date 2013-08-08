
package com.adnetik.userindex;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

public class ScoreUserJob extends Configured implements Tool
{
	public enum ScoreCounter { InfScores };
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{		
		// Make sure we are using both .txt and .lzo
		// getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);				
		
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());		
		String partcode = args[0];
		partcode = Util.padLeadingZeros(Integer.valueOf(partcode), 5); 
		String blockend = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
		
		// Allow to run for other candays if necessary
		Map<String, String> argmap = Util.getClArgMap(args);
		if(argmap.containsKey("blockend"))
		{ 
			blockend = argmap.get("blockend"); 
			Util.pf("Using specially specified blockend %s\n", blockend);
			UserIndexUtil.assertValidBlockEnd(blockend);
		}
		
		{
			// This is a FAIL-FAST check to ensure that the classifier information 
			// is set up correctly before running the job.
			Map<String, AdaBoost<UserPack>> pix2class = UserIndexUtil.readClassifData(fSystem, blockend);
			Util.pf("\nFound classifiers for pixel ids: %s", pix2class.keySet());
			Util.massert(!pix2class.isEmpty(), 
				"No classifiers found for blockend=%s", blockend);
		}
					
		{
			List<String> daylist = UserIndexUtil.getCanonicalDayList(blockend);
			
			List<Path> pathlist = Util.vector();
			for(String daycode : daylist)
			{
				Path spath = new Path(Util.sprintf("/userindex/sortscrub/%s/part-%s.%s", daycode, partcode, UserIndexUtil.PART_SUFF));				
				Util.massert(fSystem.exists(spath), "Path not found %s", spath);
				pathlist.add(spath);
			}
						
			Util.pf("\nFound %d paths", pathlist.size());
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));
			//job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		}
					
		{
			// CheckWtpEmptyMapper ensures that the first token is a valid WTP ID
			Text a = new Text("");
			HadoopUtil.alignJobConf(job, new HadoopUtil.CheckWtpEmptyMapper(), new ScoreReducer(), a, a, a, a);
		}
		
		{
			String outputPath = Util.sprintf("/userindex/userscores/%s/shard_%s", blockend, partcode);
			HadoopUtil.moveToTrash(this, new Path(outputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		}
			
		// Annoyingly need to set this
		job.setStrings(UserIndexUtil.HadoopJobCode.BLOCK_END_DAY.toString(), blockend);		
		
		// Specify various job-specific parameters 
		job.setJobName(Util.sprintf("ScoreUserJob PartCode=%s BlockEnd=%s", partcode, blockend));
		
		job.setPartitionerClass(Reshuffler.class);

		job.setNumReduceTasks(15);
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	

		return 1;

	}

	static void popUPackFrmRdcrData(UserPack curpack, Iterator<? extends Object> values)
	{
		popUPackFrmRdcrData(curpack, values, null);
	}


	// This method reconstructs a package of user data from the information in the values iterator
	// It must understand the specific format that is used for the records in both the Slice data and the Shuf data
	static void popUPackFrmRdcrData(UserPack curpack, Iterator<? extends Object> values, Reporter repper)
	{
		while(values.hasNext())
		{
			if(curpack.getBidCount() + curpack.getPixCount() > UserIndexUtil.MAX_UPACK_SIZE)
				{ break; }
			
			Pair<String, String> dtype_logline = Util.splitOnFirst(values.next().toString(), '\t');
			
			String logline = dtype_logline._2;
			DataTypeCode dtc; 
			
			try { dtc = DataTypeCode.valueOf(dtype_logline._1); }
			catch (IllegalArgumentException iaex) { 
				checkNullSend(repper, UserIndexUtil.Counter.BadDataTypeTotal, 1);
				continue;
			}
					
			LogEntry logentry = null;
			
			if(dtc == DataTypeCode.bid)
			{	
				checkNullSend(repper, UserIndexUtil.Counter.BidTotal, 1);
							
				logentry = BidLogEntry.getOrNull(LogType.UIndexMinType, LogVersion.UIndexMinVers2, logline);
				if(logentry == null)
				{
					// Shouldn't see too many of these.
					checkNullSend(repper, HadoopUtil.Counter.LogFormatExceptions, 1);
					continue; 
				}
							
			} else if(dtc == DataTypeCode.pix) {
				
				checkNullSend(repper, UserIndexUtil.Counter.PixTotal, 1);
				
				logentry = PixelLogEntry.getOrNull(logline);
				if(logentry == null)
				{
					checkNullSend(repper, HadoopUtil.Counter.PixLogFormatExceptions, 1);
					continue; 					
				}
				
			} else {
				
				// This should only happen if we decide to add a new type of data and forget
				// to enter code for it above.
				Util.massert(false, "Unknown DataTypeCode: %s", dtc);	
			}
			
			// This is basically paranoia, but just because I'm paranoid doesn't mean they're not out to get me.
			WtpId checkwtp = WtpId.getOrNull(logentry.getField(LogField.wtp_user_id));
			if(checkwtp == null || !checkwtp.toString().equals(curpack.userId))
			{
				checkNullSend(repper, UserIndexUtil.Counter.WtpMixup, 1);
				continue;
			}			
			
			// UserPack knows how to deal with pix/bid 
			curpack.add(logentry);
		}		
	}
	
	private static void checkNullSend(Reporter reporter, Enum mycount, int val)
	{
		if(reporter == null)
			{ return; }
		
		reporter.incrCounter(mycount, val);
			
	}
	
	// The work needs to be done in the reducer, and not the mapper,
	// because the user IDs are not grouped perfectly together in the input - they are spread across several files	
	// This job DOES NOT use UFeatManager!!!
	// If the functions get into the saved AdaBoost classifiers, they are going to be
	// used for scoring. All the feature management logic must happen upstream, 
	// when the learning is done.
	public static class ScoreReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{	
		// Pixel ID --> classifier
		Map<String, AdaBoost<UserPack>> classifMap;
			
		Map<String, boolean[]> funcMap = Util.treemap();
		
		Random sanRand = new Random();
		
		@Override
		public void configure(JobConf job)
		{
			Util.pf("\nCONFIGURING -----------------------");
			
			try { 
				FileSystem fsys = FileSystem.get(job);
				
				String blockend = job.get(UserIndexUtil.HadoopJobCode.BLOCK_END_DAY.toString());

				classifMap = UserIndexUtil.readClassifData(fsys, blockend);
				
				//Util.pf("\nFound pixel set %
				
				initFuncMap();
				
				Util.massert(!classifMap.isEmpty(), "Empty classifier map for blockend=%s", blockend);
				
			} catch (Exception ex) {
				
				Util.pf("\nError configuring filter");
				throw new RuntimeException(ex);
			}
		}
		
		private void initFuncMap()
		{
			for(String listcode : classifMap.keySet())
			{
				for(String onefunc : classifMap.get(listcode).getBaseNameKeyList())
				{
					// evaluating one user at a time, so only really need one-element array
					funcMap.put(onefunc, new boolean[1]);	
				}
			}
		}
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			{
				WtpId widkey = WtpId.getOrNull(key.toString());
				if(widkey == null)
					{ return; }
			}
			
			
			try {
				// Keys are just WTP-Ids.
				UserPack curpack = new UserPack();
				curpack.userId = key.toString();
				
				popUPackFrmRdcrData(curpack, values, reporter);
				
				// Log some info about how many callouts/user
				{
					int cocount = curpack.getBidList().size();	
					
					if(cocount == 1) { reporter.incrCounter(UserIndexUtil.Counter.OneCalloutUsers, 1); }
					if(cocount == 2) { reporter.incrCounter(UserIndexUtil.Counter.TwoCalloutUsers, 1); }
					if(cocount == 3) { reporter.incrCounter(UserIndexUtil.Counter.ThreeCalloutUsers, 1); }	
					
					if(cocount == 0)
					{
						// This should almost never happen
						reporter.incrCounter(UserIndexUtil.Counter.ZeroCalloutUsers, 1);
						return;						
					}
				}
				
				String ucountry = curpack.getFieldMode(LogField.user_country);
				Util.massert(ucountry != null && ucountry.length() == 2,
					"Bad user country '%s'", ucountry);	
				
				// The idea here is to calculate the feature results ONCE, and use those 
				// feature results to compute user scores for each classifier,
				// since all the classifiers are built on the same feature set.
				for(String namekey : funcMap.keySet())
				{
					UserFeature onefeat = UserFeature.buildFromNameKey(namekey);
					funcMap.get(namekey)[0] = onefeat.eval(curpack).getVal();
				}
				
				// Okay, this method is a bit inefficient, because we are 
				// evaluating the features 
				for(Map.Entry<String, AdaBoost<UserPack>> lc_ada : classifMap.entrySet())
				{
					// PosRequest preq = (PosRequest) ScanRequest.buildFromListCode(listcode);
					
					String listcode = lc_ada.getKey();
					AdaBoost<UserPack> ada = lc_ada.getValue();
					double score = ada.calcScore(funcMap, 0); // 0 = index of user, since we only have one user
					
					// Sanity check - this method is inefficient but doesn't depend on doing 
					// the whole funcMap prefix checking stuff correctly.
					if(sanRand.nextDouble() < .00001)
					{
						List<String> basekeylist = ada.getBaseNameKeyList();
						double checkscore = ada.calcScore(UserFeature.buildFeatMap(basekeylist), curpack);
						Util.massert(Math.abs(score - checkscore) < 1e-9);					
					}
					
					// apparently sometimes we can get infinite scores
					if(Math.abs(score) > 1e8)
					{
						reporter.incrCounter(ScoreCounter.InfScores, 1);
						continue;
					}
					
					String resval = Util.sprintf("%s\t%.05f\t%s", listcode, score, ucountry);
					collector.collect(key, new Text(resval));
				}
				
				
				reporter.incrCounter(HadoopUtil.Counter.ProcUsers, 1);
			} 
			
			catch (Exception ex) {
				
				reporter.incrCounter(HadoopUtil.Counter.GenericError, 1);
			}
		}		
	}		

	// Big Gotcha: this job runs only a single partition of WTP ids. 
	// So if you use the default partitioner, you will end up sending 
	// all the data to a single reducer.
	public static class Reshuffler implements Partitioner<Text, Text>
	{
		public void configure(JobConf jobconf) {
			//super.configure(jobconf);
		}
		
		
		public int getPartition(Text key, Text value, int numPart)
		{
			String code = key.toString().substring(0, 5);
			int hexint = Integer.parseInt(code, 16);
			return (hexint % numPart);
		}
	}
	
}

