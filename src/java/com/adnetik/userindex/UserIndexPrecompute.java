
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;

import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.*;

import com.adnetik.data_management.*;

public class UserIndexPrecompute extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{
		int mainCode = HadoopUtil.runEnclosingClass(args);
		Util.pf("\n");
	}
	
	public int run(String[] args) throws Exception
	{	
		// This solves an subtle error that heppens when reading BlueKai data.
		// Basically, if the list is short, and starts with a relatively high-ID user,
		// it will take a while to run through the lower-ID users
		getConf().setInt("mapred.task.timeout", 3600*1000);		
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		ArgMap amap = Util.getClArgMap(args);
		Util.massert(amap.containsKey("blockend"), "Must specify blockend data explicitly");
		
		String blockend = amap.getString("blockend", "xxxx");
		UserIndexUtil.assertValidBlockEnd(blockend);
		
		// Fail-fast
		{
			buildLookupMap();	
		}
		
		// Okay, since this job is the first to run in the chain of AIDX jobs,
		// it has the responsibility of building the FeatMan data
		boolean buildfeat = amap.getBoolean("buildfeat", true);
		
		if(buildfeat)
		{
			Util.pf("Building feature man for blockend=%s... ", blockend);
			{
				SimpleMail featmanmail = new SimpleMail(UserIndexUtil.AIDX_WEEKLY_CODE + " UFeatManagerReport BlockEnd=" + blockend);
				featmanmail.setPrint2Console(false);
				
				UFeatManager.buildFeatMan4Date(featmanmail, blockend);
				
				// TODO: probably isn't really necessary 
				featmanmail.send2admin();
			}
			Util.pf(" ... done\n");
		}
		
		// Listen Code --> index
		Map<String, Integer> readyMap = CheckReady.loadReadyMap(blockend);
		// Util.pf("Found listenCodeMap = %s", listenCodeMap);

		// Inputs are : 
		{
			String targliststr = amap.getString("targlist", "all");
			List<Path> pathlist = Util.vector();
			Util.pf("Found %d ready list codes\n", readyMap.size());
			
			for(String listcode : readyMap.keySet())
			{
				if((targliststr.indexOf(listcode) == -1) && !targliststr.equals("all"))
					{ continue; }
				
				for(String oneday : UserIndexUtil.getCanonicalDayList(blockend))
				{
					Path onepath = new Path(Util.sprintf("/userindex/dbslice/%s/%s.%s", oneday, listcode, UserIndexUtil.SLICE_SUFF));
					pathlist.add(onepath);
					Util.massert(fSystem.exists(onepath));
					
				}
								
				// Util.pf("Listen code %s is ready\n", listcode);
				// Util.massert(listenCodeMap.containsKey(listcode), "Listen code map does not contain %s", listcode);
			}
						
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
				
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, true, new HadoopUtil.EmptyMapper(), new PrecompReducer(), 
				new SmartPartitioner(), a, a, a, a);
		}
		
		
		Path outputPath = new Path(UserIndexUtil.getPrecompHdfsDir(blockend));
		HadoopUtil.moveToTrash(this, outputPath);
		Util.pf("\nUsing directory %s", outputPath);
		FileOutputFormat.setOutputPath(job, outputPath);			
		
		Util.pf("\nCalling UserIndexPrecompute for block-end:%s", blockend);

		// Annoyingly need to set this
		job.setStrings(HadoopJobInfo.blockend.toString(), blockend);
		
		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("UserIndexPrecompute BlockEnd=%s", blockend));
		
		// This job is basically light on the reducers, shouldn't be an issue to use a lot of them.
		job.setNumReduceTasks(readyMap.size()); 
		//job.setPartitionerClass(SliceInterestSecond.SlicePartitioner.class);
		
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
				
		// NB job can fail and not throw an error.
		if(success)
		{ 
			Util.pf("\nJob finished, renaming output files");	
			cleanNRename(readyMap, blockend, getConf());
		
		} else {
			Util.pf("\nJOB FAILED");	
		}
		
		//logMail.logOrSend(logfile);
		
		return 0;
	}
	
	static void cleanNRename(Map<String, Integer> readymap, String blockend, Configuration conf) throws IOException
	{
		UserIndexUtil.assertValidBlockEnd(blockend);
		
		String pcpath = UserIndexUtil.getPrecompHdfsDir(blockend);
		FileSystem fsys = FileSystem.get(conf);
		
		for(String listcode : readymap.keySet())
		{
			int partcode = readymap.get(listcode);
			Path srcpath = new Path(Util.sprintf("%s/part-%s", pcpath, Util.padLeadingZeros(partcode, 5)));
	
			// In previous system, would get many zero-length files, but now we shouldn't get many/any
			if(fsys.getFileStatus(srcpath).getLen() > 0)
			{ 
				Path dstpath = new Path(Util.sprintf("%s/%s.prec", pcpath, listcode));
				fsys.rename(srcpath, dstpath); 
			}
		}	
		
		// These are basically errors
		List<Path> zerolist = HadoopUtil.getGlobPathList(fsys, Util.sprintf("%s/part-*", pcpath));
		for(Path onezero : zerolist)
		{				
			Util.pf("WARNING: found part file with non-zero length %s\n", onezero);
		}
	}
	
	private static TreeMap<String, ScanRequest> buildLookupMap()
	{
		TreeMap<String, ScanRequest> scanmap = Util.treemap();
		
		for(ScanRequest screq : ListInfoManager.getSing().getFullScanReqSet())
		{
			Util.putNoDup(scanmap, screq.getListCode(), screq);
			
			// Sometimes these are the same
			if(screq.getListCode().equals(screq.getOldListCode()))
				{ continue; }
			
			scanmap.put(screq.getOldListCode(), screq);
		}			
		
		return scanmap;
	}
	
				
	
	
	// Precomputes a the feature responses for each user.
	public static class PrecompReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		int lcount = 0;
		Random _randCheck = new Random();
		
		int _userCount = 0;
		
		ScanRequest _scanReq;
		
		// TODO: remove this
		private SortedMap<String, ScanRequest> _scanReqMap;
		
		private void checkInit3PartyData() throws IOException
		{
			// Note here we are getting the CANDAY directly - that might cause some problems
			// Basically this 
			// Currently we are NOT using 3rd party data here - so we just set things to NullMode()
			if(!BluekaiDataMan.isQReady())
			{
				String canday = UserIndexUtil.getCanonicalEndDaycode();	
				BluekaiDataMan.setSingQ(canday);
				BluekaiDataMan.getSingQ().setNullMode();				
			}
			
			if(!ExelateDataMan.isQReady())
			{
				String canday = UserIndexUtil.getCanonicalEndDaycode();	
				ExelateDataMan.setSingQ(canday);
				ExelateDataMan.getSingQ().setNullMode();
			}								
		}
		
		private void checkInitFeatMan(Reporter rep) throws IOException
		{
			if(!UFeatManager.isSingReady())
			{
				String blockend = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
				UFeatManager.initSing(blockend);
				rep.incrCounter(UserIndexUtil.Counter.FeatManInit, 1);
			}
		}
		
		// TODO: should take this out, only need it because of transition
		// from old to new listcode naming scheme (pixel_xxxx vs user_xxx)
		private ScanRequest lookupScanReq(String listcode)
		{
			if(_scanReqMap == null)
			{
				_scanReqMap = buildLookupMap();
			}
			
			return _scanReqMap.get(listcode);
		}
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			// Skip if the user count is greater than the max useful number
			if(_userCount >= UserIndexUtil.MAX_USERS_PRECOMP)
				{ return; }
			
			checkInitFeatMan(reporter);
			
			// Now we just DON'T initialize the 3rd party data, and we'll be fine
			// checkInit3PartyData();
			
			// Basic format of line is <listcode____WTP, DataTypeCode, logline>
			String[] listcode_wtp = key.toString().split(Util.DUMB_SEP);
			WtpId userwtp = (listcode_wtp.length < 2 ? null : WtpId.getOrNull(listcode_wtp[1]));
			
			// Check that key is correctly formatted.
			if(userwtp == null)
			{ 
				reporter.incrCounter(UserIndexUtil.Counter.BadWtp, 1);
				return;
			}
			
			ScanRequest myrequest = lookupScanReq(listcode_wtp[0]);
			
			// TODO: change back to old scheme
			if(_scanReq == null)
			{
				// Okay, this is the lightweight, "skeleton" version of the scan request.
				// Just the basics, hopefully this should be enough to get the job done.
				// _scanReq = ScanRequest.buildFromListCode(listcode_wtp[0]);
				
				_scanReq = myrequest;
				
			} 
			
			// TODO: change back to old convention
			// All data for a single listcode gets sent to the same reducer
			//Util.massert(_scanReq.getListCode().equals(listcode_wtp[0]),
			//	"Mixed data in reducer, found scanrequest %s and listcode %s",
			//	_scanReq.getListCode(), listcode_wtp[0]);
			
			Util.massert(myrequest != null, "Failed to find scanreq for listcode %s", listcode_wtp[0]);
			Util.massert(myrequest == _scanReq, 
				"Found listcode %s in package with scanrequest new=%s/old=%s",
				listcode_wtp[0], _scanReq.getListCode(), _scanReq.getOldListCode());
			
			try {			
				UserPack curpack = new UserPack();
				curpack.userId = listcode_wtp[1];
				
				ScoreUserJob.popUPackFrmRdcrData(curpack, values, reporter);
				
				// Util.pf("\nFound %d callouts for user %s", curpack.getData().size(), curpack.userId);
				List<String> keyval = getKeyValList(curpack, _scanReq, _randCheck);
						
				lcount++;
				collector.collect(new Text(curpack.userId), new Text(Util.join(keyval, "\t")));
				reporter.incrCounter(HadoopUtil.Counter.ProcUsers, 1);
				
				// Log how much time we've spent looking up Bluekai data in the Q
				long lookuptime = (BluekaiDataMan.isQReady() ? BluekaiDataMan.getSingQ().lastLookupTimeMillis() : 0);
				reporter.incrCounter(BluekaiDataMan.BluekaiEnum.LookupTimeMillis, lookuptime); 
				
				// Report on how much 3rd party data we've found
				reporter.incrCounter(HadoopUtil.Counter.FoundBlueUsers, (curpack.getBluePack() == null ? 0 : 1));
				reporter.incrCounter(HadoopUtil.Counter.FoundExelUsers, (curpack.getExelatePack() == null ? 0 : 1));
			
				_userCount++;
				
				if(_userCount == UserIndexUtil.MAX_USERS_PRECOMP)
					{ reporter.incrCounter(UserIndexUtil.Counter.HitUserMax, 1); }
				
			} catch (Exception ex) {
				
				// throw new RuntimeException(ex);
				reporter.incrCounter(HadoopUtil.Counter.GenericError, 1);	
			}
		}		
	}		
	
	
	static List<String> getKeyValList(UserPack curpack, ScanRequest scanreq, Random randcheck)
	{
		List<String> keyval = Util.vector();	
		
		// Hack: add user_country and user_region 
		{
			for(LogField onef : new LogField[] { LogField.user_country, LogField.user_region })
			{
				String mres = curpack.getFieldMode(onef);	
				if(mres == "") 
					{ mres = "??"; }
				keyval.add(onef + "=" + mres); 
			}
		}				
		
		for(UserFeature ufeat : UFeatManager.getSing().getFeatSet(scanreq))
		{
			EvalResp result = ufeat.eval(curpack);
			
			// This key is just for error checking purposes 
			int k = UserIndexUtil.funcCode(ufeat.getNameKey());
			int v = (result == EvalResp.NODATA ? 0 : (result.getVal() ? 1 : -1));
			
			// Only add the key-value check on a small percentage of columns.
			if(randcheck.nextDouble() < .005)
				{ keyval.add(k + "=" + v); }
			else 
				{ keyval.add(""+v); }
		}		
		
		return keyval;
	}
}
