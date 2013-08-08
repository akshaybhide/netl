
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;


import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


public class AidxRegTest
{
	SimpleMail _logMail;
	
	String _dayCode;
	
	Map<String, UFeatManager> _ufeatManMap = Util.treemap();
	
	FileSystem _fSystem; 
	
		
	public static void main(String[] args) throws IOException
	{
		String daycode = TimeUtil.getYesterdayCode();
		
		AidxRegTest art = new AidxRegTest(daycode);
				
		art.runFullTestSuite();
		
	}
	
	AidxRegTest(String dc) throws IOException
	{
		TimeUtil.assertValidDayCode(dc);
		
		_dayCode = dc;
		
		_logMail = new SimpleMail("AidxRegTestMail for " + _dayCode);
		
		_fSystem = FileSystem.get(new Configuration());
	}
	
	void runFullTestSuite() throws IOException
	{
		try {
			PrecAlignTest pat = new PrecAlignTest();
			pat.runTest();					
			
			FeaturePermTest fpt = new FeaturePermTest();
			fpt.runTest();					
			
			SlicePresentTest spt = new SlicePresentTest();
			spt.runTest();
		
			StagingListTest slt = new StagingListTest();
			slt.runTest();
			
			checkDataMixOkay();			
			
	
			
			PrecompFeatTest pft = new PrecompFeatTest();
			pft.runTest();
			
			AdaPermTest apt = new AdaPermTest();
			apt.runTest();
		
			
		} catch (Exception ex) {
			
			_logMail.addExceptionData(ex);
		}
		_logMail.send2admin();
	}
	
	protected UFeatManager getFeatMan(String be) throws IOException
	{
		UserIndexUtil.assertValidBlockEnd(be);
		
		Util.setdefault(_ufeatManMap, be, new UFeatManager(be));
		
		return _ufeatManMap.get(be);
	}
	
	// Check that we have BOTH pixel data and bid data in the SHUF/SLICE files
	// We have often run into situations where the pixel data is present but the bid data isn't.
	void checkDataMixOkay() throws IOException
	{
		String probeday = findTestInfoDay();
		_logMail.pf("Running mix-data check for %s\n", probeday);
		
		// Check part file
		{
			String partfile = LocalMode.partPathFromId(0, probeday);
			BufferedReader bread = FileUtils.getGzipReader(partfile);
			
			int[] counts = new int[2];
			
			for(int lc = 0; lc < 1000000; lc++)
			{	
				String oneline = bread.readLine();
				
				String[] toks = oneline.split("\t");
				DataTypeCode dcode = DataTypeCode.valueOf(toks[1]);
			
				counts[(dcode == DataTypeCode.bid ? 0 : 1)]++;				
			}
			
			bread.close();
			
			if(counts[0] == 0 || counts[1] == 0)
				{  _logMail.pf("*****ERROR DETECTED******\n"); }
			
			_logMail.pf("Found %d bids, %d pixs in 100000 lines for probeday %s\n", 
				counts[0], counts[1], probeday);
		}
		
		
	}
	
	
	
	// Go backwards in the SHUF data, until we find a day where the mod times are substantially in the past.
	// The point is that we don't want to synchronize closely with the timing of the LocalMode slice job
	private String findTestInfoDay()
	{
		String probe = _dayCode;
		
		double curtime = Util.curtime();
		double okaygap = 24*60*60*1000; // One day in milliseconds
		
		
		for(int i = 0; i < 100; i++)
		{
			File partfile = new File(LocalMode.partPathFromId(0, probe));
			
			if(partfile.exists() && (curtime - partfile.lastModified()) > okaygap)
				{ return probe; }
			
			probe = TimeUtil.dayBefore(probe);			
		}
		
		throw new RuntimeException("Could not find good part file on local disk");
	}
	
	
	public class FeaturePermTest
	{
		String _blockEnd;
		
		SortedSet<Integer> _repIdSet = Util.treeset();
				
		FeaturePermTest()
		{
			_blockEnd = getBlockEnd();
			
			_logMail.pf("Running FeaturePermTest for blockend %s\n", _blockEnd);
		}
		
		// Want to test the most recent block end date that has data in the feature table.
		String getBlockEnd()
		{
			String sql = "select max(RI.can_day) from feature_table FT join report_info RI on FT.report_id = RI.report_id"; 
			List<java.sql.Date> oneres = DbUtil.execSqlQuery(sql, new UserIdxDb());
			return oneres.get(0).toString();
		}
		
		void initReportIdSet()
		{
			// Okay, the issue with report_info table is that we are kind of promiscuous in terms of 
			// assigning report IDs. Want to check only for listcodes that actually have data.
			// String sql = Util.sprintf("SELECT report_id FROM report_info WHERE can_day = '%s'", _blockEnd);
			String sql = Util.sprintf("SELECT RI.report_id FROM report_info RI join feature_table FT on RI.report_id = FT.report_id");
			sql += Util.sprintf(" WHERE RI.can_day = '%s' group by RI.pos_list_code", _blockEnd);
			
			List<Integer> replist = DbUtil.execSqlQuery(sql, new UserIdxDb());
			_repIdSet.addAll(replist);
			
			_logMail.pf("Found %d report IDs for blockend day %s\n", _repIdSet.size(), _blockEnd);
		}
		
		// Okay, 
		void runTest() throws IOException
		{
			UFeatManager ufeatman = getFeatMan(_blockEnd);
			
			initReportIdSet();
						
			int numerror = 0;
			
			for(int repid : _repIdSet)
			{
				PosRequest posreq = getPosReq4Id(repid);

				if(!ufeatman.havePosRequest(posreq))
				{
					_logMail.pf("WARNING: posreq %s in DB but not FEATMAN\n", posreq.getListCode());
					numerror++;
					continue;	
				}
				
				Set<String> featnameset = Util.treeset();
				
				// Util.pf("Testing for scanreq %s\n", posreq.getListCode());
				
				for(UserFeature onefeat : ufeatman.getFeatSet(posreq))
				{
					// This is the DISPLAY version not the name key
					Util.putNoDup(featnameset, onefeat.toString());
					// featnameset.add(onefeat.toString());	
				}
								
				Set<String> dbnameset = featNames4RepId(repid);
				for(String onedbname : dbnameset)
				{
					if(!featnameset.contains(onedbname))
					{	
						_logMail.pf("WARNING, feature %s in DB but not FEATMAN for repid=%d, listcode=%s, blockend=%s\n", 
							onedbname, repid, posreq.getListCode(), _blockEnd);
						numerror++;
						
						if(numerror > 1000)
							{ return; }
					}
				}
			}
			
			_logMail.pf("Checked %d report IDs, found %d errors\n", _repIdSet.size(), numerror);
			
			//_logMail.pf("Found listcode %s for report id %d, %d features. %d errors \n", 
			//	posreq.getListCode(), repid, featnameset.size(), numerror);				
			
		}
		
		Set<String> featNames4RepId(int repid)
		{
			String sql = Util.sprintf("SELECT feat_name FROM feature_table WHERE report_id = %d", repid);
			List<String> featlist = DbUtil.execSqlQuery(sql, new UserIdxDb());
			return new TreeSet<String>(featlist);
		}
		
		PosRequest getPosReq4Id(int repid) throws IOException
		{
			String sql = Util.sprintf("SELECT pos_list_code FROM report_info WHERE report_id = %d", repid);
			List<String> replist = DbUtil.execSqlQuery(sql, new UserIdxDb());
			return ListInfoManager.getSing().getPosRequest(replist.get(0));
		}
	}
	
	// Check that all the cookie lists in the staging directory are actual cookie lists
	public class StagingListTest
	{
		
		public void runTest()
		{
			String blockstart = UserIndexUtil.getBlockStartForDay(_dayCode);
			
			try { 
				List<ScanRequest> cklist = LookupPack.grabStagingListCodes(_fSystem, blockstart);
				_logMail.pf("Found %d valid cookie lists\n", cklist.size());
				
			} catch (Exception ex) {
				
				_logMail.pf("*****ERROR DETECTED*****\n");
				_logMail.addExceptionData(ex);
			}
		}
	}
	
	
	public class AdaPermTest
	{
		String _blockEnd;
		
		int _numErr = 0;
				
		AdaPermTest() throws IOException
		{
			_blockEnd = getBlockEnd();
			
			_logMail.pf("Running AdaPermTest for blockend %s\n", _blockEnd);
		}
		
		// Want to test the most recent block end date that has data in the feature table.
		String getBlockEnd() throws IOException
		{
			String probe = "2013-01-01";
			String mostrecent = null;
			
			for(int i = 0; i < 1000; i++)
			{
				probe = TimeUtil.dayAfter(probe);
				
				if(!UserIndexUtil.isBlockEndDay(probe))
					{ continue; }
				
				Path adadir = new Path(UserIndexUtil.getHdfsAdaClassDir(probe));
				
				if(_fSystem.exists(adadir))
				{
					// Util.pf("Found ada dir %s\n", adadir);	
					mostrecent = probe;
				}
			}
			
			Util.massert(mostrecent != null, "Failed to find any HDFS AdaClass directories");
			return mostrecent;
		}
		
		
		
		// Okay, 
		void runTest() throws IOException
		{
			Map<String, AdaBoost<UserPack>> classmap = UserIndexUtil.readClassifData(_fSystem, _blockEnd);
			
			_logMail.pf("Checking permissions for %d classifiers\n", classmap.size());
			
			UFeatManager ufeatman = getFeatMan(_blockEnd);
			
			for(String listcode : classmap.keySet())
			{
				Util.massert(ListInfoManager.getSing().haveRequest(listcode),
					"List code %s not found", listcode);
				
				ScanRequest scanreq = ListInfoManager.getSing().getRequest(listcode);
				SortedSet<UserFeature> featset = ufeatman.getFeatSet(scanreq);
				
				for(String namekey : classmap.get(listcode).getBaseNameKeyList())
				{
					UserFeature ufeat = UserFeature.buildFromNameKey(namekey);
					
					if(!featset.contains(ufeat))
					{
						_logMail.pf("ERROR: feature %s found in classifer but not FEATMAN for listcode %s :: %s\n",
							namekey, listcode, _blockEnd);
						_numErr++;
					}
				}
				
				// _logMail.pf("Scan Request %s checked out okay\n", scanreq.getListCode());
			}
			
			_logMail.pf("Checked AdaClass Perm info for %d classifiers, %d errors\n",
				classmap.size(), _numErr);
		}
	}	
	
	public class PrecompFeatTest
	{
		String _blockEnd;
		
		int _numErr = 0;
				
		PrecompFeatTest() throws IOException
		{
			_blockEnd = getBlockEnd();
			
			_logMail.pf("Running PrecompFeatTest for blockend %s\n", _blockEnd);
		}
		
		// Want to test the most recent block end date that has data in the feature table.
		// TODO: replace with HadoopUtil.getDateSortedPathMap
		String getBlockEnd() throws IOException
		{
			String probe = "2013-01-01";
			String mostrecent = null;
			
			for(int i = 0; i < 1000; i++)
			{
				probe = TimeUtil.dayAfter(probe);
				
				if(!UserIndexUtil.isBlockEndDay(probe))
					{ continue; }
				
				Path precdir = new Path(UserIndexUtil.getHdfsPrecompDir(probe));
				
				if(_fSystem.exists(precdir))
				{
					// Util.pf("Found ada dir %s\n", adadir);	
					mostrecent = probe;
				}
			}
			
			Util.massert(mostrecent != null, "Failed to find any HDFS AdaClass directories");
			return mostrecent;
		}
		
		
		
		// Okay, for each PRECOMP file, check that the number of columns in the file matches the number of features in the FEATMAN
		void runTest() throws IOException
		{
			int scorecount = 0;
			int numerr = 0;
			UFeatManager ufeatman = getFeatMan(_blockEnd);
			
			for(String listcode : ListInfoManager.getSing().getFullListCodeSet())
			{
				ScanRequest scanreq = ListInfoManager.getSing().getRequest(listcode);
				
				Path precpath = new Path(UserIndexUtil.getPrecompHdfsPath(_blockEnd, scanreq));
				
				if(!_fSystem.exists(precpath))
					{ continue; }
				
				scorecount++;
				
				SortedSet<UserFeature> featset = ufeatman.getFeatSet(scanreq);
				
				Util.pf("Found precpath %s for listcode %s\n", precpath, listcode);
					
				BufferedReader bread = HadoopUtil.hdfsBufReader(_fSystem, precpath);
				List<String> top100 = FileUtils.readNLines(bread, 100);			
				bread.close();
				
				for(String onetop : top100)
				{
					String[] toks = onetop.split("\t");	
					
					// There are 3 non-function fields: cookie, user_country, user_region
					// 006da73c-f172-4514-bafe-596c826b78e0    user_country=ES user_region=52  -1      -1      -1      -1
					int numfunc = toks.length - 3;
					
					if(numfunc != featset.size())
					{
						_logMail.pf("ERROR: Found %d tokens = %d functions in PREC file, %d functions in featset",
									toks.length, numfunc, featset.size());
						
						numerr++;
					}
				}
			}
			
			_logMail.pf("Checked integrity of %d precomp files, %d errors\n",
				scorecount, numerr);			
		}
	}	
	
	// Prevent partial-uploading errors. 
	// For unknown reasons, sometimes the scan succeeds for most of a day, but a couple
	// of files simply aren't uploaded.
	public class SlicePresentTest
	{
		int _checkOkay = 0;
		int _checkTotl = 0;
		
		List<String> _targList = Util.vector();
		
		SlicePresentTest() throws IOException
		{
			String slicepatt = "/userindex/dbslice/*/*.slice.gz";
			SortedMap<String, SortedSet<Path>> pathmap = HadoopUtil.getDateSortedPathMap(_fSystem, slicepatt);
						
			Util.massert(!pathmap.isEmpty(), "Failed to find any precompute paths for pattern %s", slicepatt);
		
			// get rid of most recent, b/c it might be half full
			LinkedList<String> daylist = new LinkedList<String>(pathmap.keySet());
			daylist.pollLast();
			
			while(_targList.size() < 10)
				{ _targList.add(daylist.pollLast()); }
			
			_logMail.pf("Initialized SlicePresentTest with daylist=%s\n", _targList);
		}
		
		public void runTest() throws IOException
		{
			for(ScanRequest scanreq : ListInfoManager.getSing().getFullScanReqSet())
			{
				for(String targday : _targList)
					{ checkReq4Day(scanreq, targday); }
			}
					
			_logMail.pf("SlicePresentTest found %d / %d slice paths okay\n",
				_checkOkay, _checkTotl);
		}
		
		
		private void checkReq4Day(ScanRequest screq, String probeday) throws IOException
		{
			String blockstart = UserIndexUtil.getBlockStartForDay(probeday);
			
			//Util.pf("Checking slice data for %s, probe %s, start %s\n",
			//	screq.getListCode(), probeday, blockstart);
			
			Path stagepath = new Path(UserIndexUtil.getStagingInfoPath(screq, blockstart));
			Path slicepath = new Path(UserIndexUtil.getHdfsSlicePath(screq, probeday));
			
			if(_fSystem.exists(stagepath))
			{
				_checkTotl++;
				
				if(!_fSystem.exists(slicepath))
				{
					_logMail.pf("******ERROR********: stage path %s exists but slice path %s is missing\n",
						stagepath, slicepath);
				} else {
					
					_checkOkay++;
				}
			}
		}
	}
	
	// Checks that the Precompute files are all "lined up" okay.
	// In other words, the precompute files correspond to the staging files.
	// Hmmm, suppose I could just put explicit listcode information in the precomp files themselves,
	// would only add a small amount to the total size of files.
	// TODO: this is going to work anymore because of multipixel list requests. 
	// replace with the actual listcode in the precomp data, shouldn't be a huge increase in data size
	public class PrecAlignTest
	{
		String _blockEnd;
				
		PrecAlignTest() throws IOException
		{
			String precpatt = "/userindex/precomp/*/*.prec";
			SortedMap<String, SortedSet<Path>> pathmap = HadoopUtil.getDateSortedPathMap(_fSystem, precpatt);
			
			Util.massert(!pathmap.isEmpty(), "Failed to find any precompute paths for pattern %s", precpatt);
			
			_blockEnd = pathmap.lastKey();
			_logMail.pf("Found blockend=%s for precompute directories\n", _blockEnd);
		}
		
		void runTest() throws IOException
		{
			String blockstart = UserIndexUtil.getBlockStartForDay(_blockEnd);
			LookupPack lpack = new LookupPack(blockstart);
			
			if(!lpack.stagingInfoReady(_fSystem))
			{ 
				_logMail.pf("LookupData not ready for blockstart %s\n", blockstart);	
				return;
			}
			
			lpack.readFromHdfs(_fSystem);
			int checkokay = 0, checktotl = 0;
			
			for(String listcode : ListInfoManager.getSing().getFullListCodeSet())
			{
				ScanRequest scanreq = ListInfoManager.getSing().getRequest(listcode);
				
				String precpath = UserIndexUtil.getPrecompHdfsPath(_blockEnd, scanreq);
				
				// Normal that some prec files will not be present
				if(!_fSystem.exists(new Path(precpath)))
					{ continue;} 
				
				ScanRequest guessreq = guessRequest(_fSystem, lpack, precpath, _logMail);
				
				checktotl++;
				
				// These actually ARE the same exact object, 
				// because access to these things are all controlled by the ListInfoManager
				if(guessreq != scanreq)
				{
					_logMail.pf("*******ERROR*********, found guess request %s for scanreq %s",
						guessreq.getListCode(), scanreq.getListCode());
					
					continue;
				}
				
				checkokay++;
			}
			
			_logMail.pf("PrecAlignTest: found %d / %d aligned precomp files okay\n", checkokay, checktotl);
		}
	}

	static ScanRequest guessRequest(FileSystem fsys, LookupPack lpack, int partid, String blockend, SimpleMail logmail) throws IOException
	{
		String precpath = Util.sprintf("/userindex/precomp/%s/part-%s",
			blockend, Util.padLeadingZeros(partid+"", 5));

		return guessRequest(fsys, lpack, precpath, logmail);		
	}
	
	static ScanRequest guessRequest(FileSystem fsys, LookupPack lpack, String precpath, SimpleMail logmail) throws IOException
	{		
		// Check if the file is valid
		{
			Path hdfsprec = new Path(precpath);	
			if(!fsys.exists(hdfsprec) || fsys.getFileStatus(hdfsprec).getLen() == 0)
			{ 
				Util.pf("Prec path %s does not exist or is zero length, skipping\n", precpath);
				return null;	
			}
		}
		
		Util.pf("Checking precpath %s\n", precpath);
		
		// Going to remove elements from this thing 
		SortedSet<String> probeset = ListInfoManager.getSing().getFullListCodeSet();		
		
		BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, precpath);

		int okaycount = 0;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			Pair<String, String> recpair = Util.splitOnFirst(oneline, "\t");
			WtpId wid = new WtpId(recpair._1);
			
			Collection<String> foundlist = lpack.lookupId(wid);
			
			int prevsize = probeset.size();
			probeset.retainAll(foundlist);
				
			if(prevsize != probeset.size())
			{
				Util.pf("Prev size was %d, new size is %d, foundlist is %s\n",
					prevsize, probeset.size(), foundlist.size());
			}
			
			if(probeset.size() == 1)
				{ okaycount++; }
			
			if(okaycount > 100)
				{ break; }
		}
		
		bread.close();	
		
		// This might not be a disaster
		if(probeset.size() != 1)
		{
			logmail.pf("****WARNING*****: Found valid listcode set %s for precpath %s\n",
				probeset, precpath);
		}
		
		return ListInfoManager.getSing().getRequest(probeset.first());
	}
	
}
