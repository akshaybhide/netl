
package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.sql.*;

// xxx

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import java.text.SimpleDateFormat;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.shared.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

import com.adnetik.data_management.BluekaiDataMan;


public class UIndexMain
{	
	public enum CLarg  { uploadada, buildfeatman, dologmailtest, lzologsync, testlistsize, domaincompile, renameprecomp, showfeatset, dorename, checkfeatset, buildspec, testlookup } 
	
	// Check that the staging files are include the listen codes 
	private static void checkStaging(String daycode) throws IOException
	{
		Configuration conf = new Configuration();
		Set<String> listcodes = ListInfoManager.getSing().getListenCodeMap().keySet();
		Set<String> foundset = Util.treeset();
		
		for(StagingType onetype : StagingType.values())
		{
			// TODO:
			/*
			Path p = new Path(UserIndexUtil.getStagingInfoPath(onetype, daycode));
			
			BufferedReader bread = HadoopUtil.hdfsBufReader(FileSystem.get(conf), p);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				String[] wtp_list = oneline.split("\t");

				if(!foundset.contains(wtp_list[1]))
				{
					Util.pf("Found listcode %s\n", wtp_list[1]);
					foundset.add(wtp_list[1]);					
				}
			}
			bread.close();
			*/
		}
		
		listcodes.removeAll(foundset);
		
		for(String notfound : listcodes)
			{ Util.pf("Did not find listcode %s in staging\n", notfound);	}
	
		Util.pf("Failed to find %d listcodes\n", listcodes.size());		
	}
	
	static void checkAlphaValues() throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		String serpatt = "/userindex/adaclass/2012-08-19/*.ser";
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, serpatt);
		
		for(Path onepath : pathlist)
		{
			// Util.pf("One path is %s\n", onepath);
			
			if(onepath.toString().indexOf("Alpha") > -1)
			{
				// Util.pf("-------------------\nOne Funcs path is %s\n", onepath);
				List<Double> alphalist = HadoopUtil.unserializeEat(fsys, onepath);
				// Util.pf("Read %d functions from path\n", funclist.size());
				
				for(Double onealpha : alphalist)
				{
					if(onealpha > 100)
					{
						Util.pf("Alpha value is %f for path %s\n", onealpha, onepath);
						break;	
					}
				}
			}
			
		}		
	}

	static void bluekaiFeatInfo() throws IOException
	{
		BluekaiDataMan.TaxonomyInfo taxinf = BluekaiDataMan.getTaxonomy();
		
		Util.pf("Found %d features to use\n", taxinf.getFeatIdSet().size());
		
		for(Integer onefeat : taxinf.getFeatIdSet())
		{
			// Util.pf("Feature id: %d, name is %s\n", onefeat, taxinf.getFeatName(onefeat));	
			
		}
	}
	
	static void bluekaiTimeTest() throws IOException
	{
		Util.pf("Going to do a blue kai time test\n");
		
		List<String> testids = FileUtils.readFileLinesE("test_ids.txt");
		
		// BluekaiDataMan.BlueUserQueue bque = new BluekaiDataMan.BlueUserQueue();
		/*
		{
			BluekaiDataMan.BlueUserQueue bque = new BluekaiDataMan.BlueUserQueue("MASTER_LIST.txt");
			BluekaiDataMan.setSingQ(bque);
		}
		*/
		
		Util.pf("Read %d testids\n", testids.size());
		
		for(int i : Util.range(testids))
		{
			String oneid = testids.get(i);
			
			BluekaiDataMan.BluserPack bup = BluekaiDataMan.getSingQ().lookup(oneid);

			if(((i+1) % 10) == 0)
			{
				double ttime = BluekaiDataMan.getSingQ().totLookupTimeSecs();
				Util.pf("Lookup completed for %d users, total time is %.03f, average is %.03f\n",
					i+1, ttime, ttime/(i+1));
			}
		}
	}
	
	static void uploadAdaInfo(String blockend) throws IOException
	{
		Util.massert(false, "Must refactor");
		/*
		FileSystem fsys = FileSystem.get(new Configuration());
		Map<String, AdaBoost<UserPack>> classifmap = UserIndexUtil.readClassifData(fsys, blockend);
		
		Util.pf("Read %d classifiers from HDFS\n", classifmap.size());
		
		UserIdxDb.uploadClassifData(classifmap, blockend);
		*/
	}
	

	
	
	public static void main(String[] args) throws Exception
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		CLarg myarg = CLarg.valueOf(args[0]);
		ArgMap amap = Util.getClArgMap(args);
		
			
		if(myarg == CLarg.checkfeatset) {
			
			String blockend = amap.getString("blockend", UserIndexUtil.getCanonicalEndDaycode());
			UFeatManager.initSing(blockend);
			
			List<ScanRequest> readyreq = UserIndexUtil.getReadyPrecompList(blockend);
			
			for(ScanRequest screq : readyreq)
			{
				Util.pf("Going to check for screq %s\n", screq.getListCode());	
				String precpath = UserIndexUtil.getHdfsPrecompPath(screq, blockend); 
				BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, precpath);
				List<String> toplist = FileUtils.readNLines(bread, 10);
				bread.close();
				
				for(String oneline : toplist)
				{
					int ntoks = oneline.split("\t").length;	
					int numfeat = UFeatManager.getSing().getFeatSet(screq).size();
					Util.massert(ntoks == numfeat+3, 
						"For SCreq %s found %d tokens but %s features",
						screq.getListCode(), ntoks, numfeat);
				}
				
				Util.pf("Scan request %s checked out okay\n", screq.getListCode());
			}
			
			
		} else if(myarg == CLarg.dorename) {
			
			List<String> renamelist = FileUtils.readFileLinesE("Renaming2.txt");
			Map<String, Integer> remap = Util.treemap();
			
			for(String oneitem : renamelist)
			{
				String[] toks = oneitem.split("\t");
				ScanRequest screq = ScanRequest.buildFromListCode(toks[0]);
				Integer partid = Integer.valueOf(toks[1]);
				remap.put(screq.getListCode(), partid);
			}
			
			Util.pf("REMAP is %s\n", remap);
			
			UserIndexPrecompute.cleanNRename(remap, "2013-03-17", new Configuration());
			
			
		} else if(myarg == CLarg.showfeatset) {
			
			String listcode = args[1];
			String blockstart = args[2];
			
			ScanRequest screq = ScanRequest.buildFromListCode(listcode);
			
			
			UFeatManager.initSing(blockstart);
			SortedSet<UserFeature> featset = UFeatManager.getSing().getFeatSet(screq);
			
			for(UserFeature ufeat : featset)
			{
				Util.pf("%s\n", ufeat.getNameKey());
			}
			
		} else if(myarg == CLarg.buildspec) {
			
			for(CountryCode cc : UserIndexUtil.COUNTRY_CODES)
			{
				ScanRequest.SpecpccRequest specreq = new ScanRequest.SpecpccRequest(Util.sprintf("specpcc_%s", cc.toString().toUpperCase())); 
				
				specreq.setNickName(Util.sprintf("PCC_Converter_List_%s", cc.toString().toUpperCase()));
				// specreq.setExpirationDate("2020-01-01");
				specreq.setHasGeoSkew(cc == CountryCode.US);
				specreq.setTargSizeK(ListInfoManager.getSing().getDefaultListSizeK(cc.toString()));
				
				
				// UserIdxDb.insertNewRequest(specreq.getListCode(), cc.toString().toUpperCase(), "2000-01-01");
				specreq.persist2db(new UserIdxDb());
				
				// break;
			}
			
		} else if(myarg == CLarg.testlookup) {
			
			LookupPack lpack = new LookupPack("2013-03-24");
			
			lpack.readFromHdfs(fsys);
			
			Util.pf("List Count map is %s\n", lpack.getListCountMap());
			
			lpack.write2File(new File("testlookup.txt"));
			
		} else if(myarg == CLarg.domaincompile) {
			
			String daycode = args[1];
			
			Util.pf("Going to call domaincompile for %s\n", daycode);
			
			StagingInfoManager.doDomainCompile(daycode, new SimpleMail("NOSEND"));
			
			
		} else if(myarg == CLarg.testlistsize) {
			
			for(PosRequest preq : ListInfoManager.getSing().getPosRequestSet())
			{
				Util.pf("For preq=%s, size_in_k is %d\n",
					preq.getListCode(), preq.getTargSizeK());
				
			}
			
		} else if(myarg == CLarg.dologmailtest) {
			
			Object classobj = ListInfoManager.getSing();
			DayLogMail dlm = new DayLogMail(ListInfoManager.getSing(), "2013-03-25");
			dlm.pf("Hello, world");
			dlm.send2admin();
			
		} else if(myarg == CLarg.lzologsync) {
			
			SimpleMail probemail = new SimpleMail("NewLzoLogSyncReport PROBE");
			probemail.pf("This is a test");
			probemail.send2admin();
			
		} else if(myarg == CLarg.buildfeatman) {
			
			String daycode = args[1];
			Util.pf("Going to build feat manager for %s\n", daycode);
			
			UFeatManager.buildFeatMan4Date(new SimpleMail("GIMP"), daycode);
		} else if (myarg == CLarg.uploadada) {
			
			String blockend = args[1];
			UserIndexUtil.assertValidBlockEnd(blockend);
			
			uploadAdaInfo(blockend);
			
			
		}
		
		
		
		/*
		
		Map<String, String> pos2neg = UserIndexUtil.getPos2NegMap();
		Map<String, String> optargs = Util.getClArgMap(args);
		String canonday = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
		
		if("adareport".equals(args[0])) {
			
			FileSystem fsys = FileSystem.get(new Configuration());
			String canday = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
			Map<String, AdaBoost<UserPack>> classifMap = UserIndexUtil.readClassifData(fsys, canday);
			
			for(String listcode : classifMap.keySet())
			{
				AdaBoost<UserPack> adab = classifMap.get(listcode);
				Util.pf("Found listcode %s with %d features\n", listcode, 
					adab.funcs.size());
				
				List<String> writelist = Util.vector();
				for(int i = 0; i < adab.funcs.size(); i++)
				{
					String oneline = Util.sprintf("%s\t%.04f", adab.funcs.get(i), adab.alpha.get(i));
					writelist.add(oneline);
				}
				
				String adausepath = UserIndexUtil.getAdaUsedFeaturePath(listcode, pos2neg.get(listcode),  TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay()));			
				FileUtils.createDirForPath(adausepath);
				FileUtils.writeFileLines(writelist, adausepath);
			}
				
		} else if("showlistenmap".equals(args[0])) {
			
			Map<String, Integer> listenmap = ListInfoManager.getSing().getListenCodeMap();
			
			for(String onecode : listenmap.keySet())
			{
				Util.pf("ListCode=%s\t\t%d\n", onecode, listenmap.get(onecode));
			}
			
		} else if("showpos2neg".equals(args[0])) {
						
			for(String onecode : pos2neg.keySet())
			{
				Util.pf("Listcode %s compares to %s\n", onecode, pos2neg.get(onecode));
			}			
			
		} else if("checkbluekai".equals(args[0])) {
			
			checkBlueKai();
			
		} else if("bluekaifeatinfo".equals(args[0])) {
			
			bluekaiFeatInfo();	
			
		} else if("cutoffmap".equals(args[0])) {

			Util.pf("Cutoff map is %s\n", UserIndexUtil.getWtpCutoffMap());
			
		} else if("timebluekai".equals(args[0])) {
			
			bluekaiTimeTest();
			
		} else if("checkstaging".equals(args[0])) {
			
			checkStaging(args[1]);
		
		} else if("showfeatures".equals(args[0])) {
			
			for(FeatureCode fcode : FeatureCode.values())
			{
				Util.pf("Feature code %s has value %b\n", fcode, fcode.isThirdParty());
			}
			
		} else if("ser2txt".equals(args[0])) {
			
			transformSer2Txt();
			
		} else if("checkalpha".equals(args[0])) {
			
			checkAlphaValues(); 
			
		} else if("checknegpools".equals(args[0])) {
			
			// String[] countries = new String[] { "US" };
			FileSystem fsys = FileSystem.get(new Configuration());
			List<Path> corruptlist = Util.vector();
			
			for(CountryCode onectry : UserIndexUtil.COUNTRY_CODES)
			{
				// String mypatt = Util.sprintf("/userindex/negpools/ *  /pool_%s.txt", onectry);
				List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, mypatt);
				
				
				for(Path checkme : pathlist)
				{	
					Integer lcount = HadoopUtil.checkCorrupt(fsys, checkme);
					
					if(lcount == null)
					{ 
						Util.pf("File is corrupt: %s\n", checkme); 
						corruptlist.add(checkme);
						
						// Delete corrupt ones
						// HadoopUtil.moveToTrash(fsys, checkme);
						
					}
					else
					{ 
						Util.pf("File %s is okay, read %d lines\n", checkme, lcount); 
					}
				}
			}
			
			for(Path onepath : corruptlist)
			{
				Util.pf("File is corrupt %s\n", onepath);
				fsys.delete(onepath, false);
			}
			Util.pf("Found %d corrupt files\n", corruptlist.size());
		}
		*/
		
	}
	
}
