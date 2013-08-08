
package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.sql.*;
import java.util.regex.*;

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

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.DbUtil.*;

import com.adnetik.data_management.*;

import com.adnetik.userindex.ScanRequest.*;


public class UserIndexUtil
{	
	public enum HadoopJobInfo { blockend };
	
	public enum EvalResp { 
		T(true), F(false), NODATA(false);
	
		private boolean baseval;
		
		EvalResp(boolean bv)
		{
			baseval = bv;
			
		}
		
		public boolean getVal() 
		{
			return baseval;
		}
	};
	
	// TODO: need to think more about what this means, how we handle it.
	public static abstract class BinaryFeature<X> 
	{
		public abstract boolean evalSub(X targ);
		
		public EvalResp eval(X targ)
		{
			return evalSub(targ) ? EvalResp.T : EvalResp.F;	
		}
		
		public abstract FeatureCode getCode();
	}	
	
	public enum HadoopJobCode { BLOCK_END_DAY };
	
	// If you have reached the COMPLETED stage, you do not send any further status updates 
	public enum AdbListStatus { mynew(1), /* processing(2) */ wait2scan(5), scanning(6), scoring(7), completed(3),  failure(4) ;
	
		private int _adbCode; 
		
		AdbListStatus(int a)
		{
			_adbCode = a;	
		}
		
		public int getAdboardCode()
		{
			return _adbCode;	
		}
		
		public String getAdbString()
		{
			if(this == AdbListStatus.mynew)
				{ return "new"; }
			
			if(this == AdbListStatus.wait2scan)
				{ return "waiting to scan"; }
			
			return this.toString();
		}
	}	
	
	public enum AidxRelDb implements DbUtil.DbTable {
		
		AdBoardCopy("adnetik"),
		UserIndexMain("userindex");
		
		private String _dbName;
		
		AidxRelDb(String dbn)
		{
			_dbName = dbn;	
		}
		
		public String getDbName()
		{
			return _dbName;	
			
		}
		
		public DbMachine getMachine()  
		{
			// Currently they are all on internal machine, this could change in future
			// return DbMachine.internal;
			
			// Changed in aftermath of DB debacle
			return DbMachine.external;
		}
		
		
	} 
	
	
	
	public enum ReportType { staging, precomp, learn, scorecomplete, test, create, renew, autorefresh, m_override }	
	
	public enum DataTypeCode { bid, pix };	
	
	public enum SpecialCode { NOTFOUND };
	
	public static final int NUM_SHUF_PARTITIONS = 24;
	
	public static final int MAX_USERS_FEATURE = 20000;
	public static final int MAX_USERS_LEARN = 10000;
	
	public static final int MAX_USERS_PRECOMP = 20000;
	
	
	public static final String AIDX_WEEKLY_CODE = "AIdxWeekly";
	
	// Less than this, we don't learn
	public static final int MIN_USER_CUTOFF = 1000;
	
	public static final Set<CountryCode> COUNTRY_CODES = Util.treeset();
	
	static {
		COUNTRY_CODES.add(CountryCode.US);
		COUNTRY_CODES.add(CountryCode.GB);
		COUNTRY_CODES.add(CountryCode.BR);
		COUNTRY_CODES.add(CountryCode.ES);
		COUNTRY_CODES.add(CountryCode.MX);
		COUNTRY_CODES.add(CountryCode.NL);
	}
	
	// Some users have MASSIVE numbers of callouts, and this can screw things up
	// So just skip over callouts or pixels if they are larger than this.
	// If users have a greater number of callout+pixels, just skip the rest
	public static final int MAX_UPACK_SIZE = 3000;
	
	
	// Need to reduce this so it doesn't take 9 million hours
	// public static final String WTP_CUTOFF = "4444444"; // really just need one "4", but this is more dramatic

	public static final String SLICE_SUFF = "slice.gz";
	public static final String PART_SUFF = "txt.gz";
	
	// Delete data that is older than this.
	public static final Integer DATA_SAVE_WINDOW = 21;	
		
	private static Map<String, String> _COUNTRY_TARG_MAP;
	
	public static final int NUM_DAY_DATA_WINDOW = 7;	

	// Default number of weeks for which to run lists
	public static final int DEFAULT_LIST_LIFETIME_WEEKS = 12;
	
	public static final int PIXEL_FIRE_LOOKBACK_DAYS = 30;
	
	// Zero-Callout should almost never happen
	public enum Counter { MaxCalloutUsers, 
				ZeroCalloutUsers, OneCalloutUsers, 
				TwoCalloutUsers, ThreeCalloutUsers, 
				FeatManInit, 
				HitUserMax, // hit the maximum number of users we're going to try
				ListCodeNotFound, // Can't find the listcode
				BidTotal, PixTotal, BadDataTypeTotal, BadWtp, WtpMixup
				};
	
	
				
				
	public enum StagingType { click, pixel, negative, 
		specpcc, pxprm,
		user, sysmulti;
		
		public boolean isPositive()
		{
			return this != StagingType.negative;	
		}
		
		public boolean isMulti()
		{
			// Pixel is really only pseudo-multi now
			// TODO
			return (this == StagingType.user || this == StagingType.pixel || this == StagingType.sysmulti);	
			
		}
	};
	
	// SYSTEM wide verticals for AIDX, we compile these into our 
	// public enum VerticalCode  { finance, auto };
	
	// TODO: need to take out the things like "hour", "browser", that
	// are really now instances of ModeMatch or SingleMatch
	public enum FeatureCode { 
		exelate(true), bluekai(true), 
		demographic, surfing_behavior, domain, 
		simplepix, // Just check if user has pixel
		calloutcount, // number of callouts
		pixel, iab,
		domain_category, // attempt to group domains into categories
		vertical, hour, browser, 
		user_region, noop, ad_exchange, os, country_region;
	
		private boolean _3party;
		
		FeatureCode(boolean is3)
		{
			_3party = is3;	
		}
		
		public boolean isThirdParty() { return _3party; }
		
		public boolean isRegionalCode() { return isRegionalCodeSub(this); }
		
		FeatureCode()
		{
			this(false);	
		}
	};
	
	public enum FeatFieldName { hour, browser, user_region, ad_exchange, domain };
	
	
	public static interface BluekaiFeatureFunc
	{
		public abstract boolean bkEval(BluekaiDataMan.BluserPack bup);
	}
	
	public static interface ExelateFeatureFunc
	{
		public abstract boolean exEval(ExelateDataMan.ExUserPack expack);
	}	
		
	
	private static boolean isRegionalCodeSub(FeatureCode fcode)
	{
		// TODO: use demographic....?
		return (fcode == FeatureCode.country_region || fcode == FeatureCode.user_region);
	}
	
	private static TreeMap<String, String> _WTP_CUTOFF_MAP = Util.treemap();

	public static Map<CountryCode, WtpId> getWtpCutoffMap()
	{
		Map<CountryCode, WtpId> cutmap = Util.treemap();
		
		for(CountryCode cc : COUNTRY_CODES)
		{
			// Boost the cutoff to F for ES, no throttling!!!
			String repwith = (cc == CountryCode.ES ? "F" : "7");
			
			WtpId cutoff = WtpId.getOrNull(Util.WTP_ZERO_ID.replaceAll("0", repwith));
			Util.massert(cutoff != null, "Failed to create cutoff object");
			cutmap.put(cc, cutoff);	
		}
		
		return cutmap;
	}
	
	public static String getWtpCutoff(String daycode)
	{
		if(_WTP_CUTOFF_MAP.isEmpty())
		{
			_WTP_CUTOFF_MAP.put("2012-10-22", "444444");			
			_WTP_CUTOFF_MAP.put("2012-10-29", "555555");
			_WTP_CUTOFF_MAP.put("2012-11-05", "666666");
			_WTP_CUTOFF_MAP.put("2012-11-12", "777777");
			// Just kidding let's stop at 77777 for the TB
			// _WTP_CUTOFF_MAP.put("2012-12-03", "888888");
			// _WTP_CUTOFF_MAP.put("2012-12-24", "999999");
		}
		
		return _WTP_CUTOFF_MAP.floorEntry(daycode).getValue();
	}
	
	
	public static final String LOCAL_UINDEX_DIR = "/local/fellowship/userindex";	
	public static final String LOCAL_PREC_DIR = LOCAL_UINDEX_DIR + "/precomp";
	public static final String LOCAL_ADA_DIR = LOCAL_UINDEX_DIR + "/adaclass";
	
	public static final String LOCAL_DBTEMP_DIR = LOCAL_UINDEX_DIR + "/db_temp";
	
	// public static String LISTEN_CODE_PATH = "/userindex/LISTEN_CODES.txt";
		
	public static String STAGING_DIR = "/userindex/staging";
	
	public static String SLICE_DIR = "/userindex/dbslice";
	
	public static String PRECOMP_DIR = "/userindex/precomp";

	
	public static final int MAX_USERS_PER_LIST = 120000; 
	
	/*
	public static void grabStage2Local(StagingType[] stagetypes, File localfile, String blockstart) throws IOException
	{
		grabStage2Local(stagetypes, localfile, blockstart, new SimpleMail("gimp"));
	}
	
	public static void grabStage2Local(StagingType[] stagetypes, File localfile, String blockstart, SimpleMail logmail) throws IOException
	{
		Util.massert(isBlockStartDay(blockstart), "Must call with block start day, you sent %s", blockstart);
		Util.massert(!localfile.exists(), "Local file %s already exists, must delete first");
		
		FileSystem fsys = FileSystem.get(new Configuration());
		BufferedWriter bwrite = FileUtils.getWriter(localfile.getAbsolutePath());
		
		for(StagingType onetype : stagetypes)
		{
			int lread = 0;
			Path stagepath = new Path(UserIndexUtil.getStagingInfoPath(onetype, blockstart));
			
			if(!fsys.exists(stagepath))
			{ 
				logmail.pf("StagingType data does not exist for %s, skipping\n", onetype);
				continue;
			}
						
			BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, stagepath);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{ 
				// Paranoid error checking, these calls will throw errors if 
				// the WTP is invalid or if the list request cannot be found
				{
					String[] wtp_lcode = oneline.trim().split("\t");
					WtpId wid = new WtpId(wtp_lcode[0]);
					Util.massert(ListInfoManager.getSing().haveRequest(wtp_lcode[1]),
						"List Code not found %s", wtp_lcode[1]);
				}
				
				
				lread++;
				bwrite.write(oneline);
				bwrite.write("\n");
			}
			bread.close();
			logmail.pf("Read %d lines for staging type %s\n", lread, onetype);
		}
		
		// Classic bug - if you forget to close the writer, the file looks like it has been truncated 
		bwrite.close();
		logmail.pf("Finished grabStage2Local, file size is %d\n",  localfile.length());
	}
	*/
	
	public static int pixelIdFromCode(String pixcode)
	{
		Util.massert(pixcode.startsWith("pixel_"), "Bad pixel list code %s", pixcode);
		return Integer.valueOf(pixcode.split("_")[1]);
	}
	
	// public static 
	public static int partFileFromListId(String listid, int numpart)
	{
		int hashcode = listid.hashCode();
		hashcode = (hashcode < 0 ? -hashcode : hashcode);
		return (hashcode % numpart);
	}
	
	public static String getStagingInfoPath(ScanRequest screq, String daycode)
	{
		return Util.sprintf("%s/%s/%s.cklist", STAGING_DIR, daycode, screq.getListCode());
	}
	
	public static List<ScanRequest> getReadyPrecompList(String blockend) throws IOException
	{
		assertValidBlockEnd(blockend);
		
		String mypatt = Util.sprintf("%s/*.prec", getHdfsPrecompDir(blockend));
		List<Path> pathlist = HadoopUtil.getGlobPathList(new Configuration(), mypatt);
		List<ScanRequest> sclist = Util.vector();
		
		for(Path onepath : pathlist)
		{
			String[] lc_prec = onepath.getName().split("\\.");
			ScanRequest screq = ListInfoManager.getSing().getRequest(lc_prec[0]);
			sclist.add(screq);
		}
		
		
		return sclist;
	}

	public static String getHdfsPrecompDir(String blockend)
	{
		assertValidBlockEnd(blockend);		
		return Util.sprintf("%s/%s", PRECOMP_DIR, blockend);
	}
	
	public static String getHdfsPrecompPath(ScanRequest scanreq, String blockend)
	{
		return Util.sprintf("%s/%s.prec", getHdfsPrecompDir(blockend), scanreq.getListCode());
	}
	
	public static String getLocalBackupDir()
	{
		return Util.sprintf("%s/backup", LOCAL_UINDEX_DIR);
	}	
	
	public static String getLocalPixelStripDir(String daycode)
	{
		return Util.sprintf("%s/strip/%s", LOCAL_UINDEX_DIR, daycode);
	}				
	
	public static String getPccListPath(String daycode, String ccode)
	{
		return Util.sprintf("%s/pcc/%s/%s.idlist", LOCAL_UINDEX_DIR, daycode, ccode);
	}					
	
	public static String getLocalPixelStripPath(String daycode, int pixelid)
	{
		return Util.sprintf("%s/pixel_%d.strip", getLocalPixelStripDir(daycode), pixelid);
	}			
	
	public static SortedSet<PosRequest> getReadyListSet(String daycode)
	{
		SortedSet<PosRequest> rset = Util.treeset();
		
		// See getLocalListPath(..) below
		Pattern aidxpath = Pattern.compile(Util.sprintf("AIM_Index_(.+)__%s.list", daycode));
		
		List<File> flist =  FileUtils.recFileCheck(getListSaveDir(daycode), aidxpath, false);
		
		for(File onefile : flist)
		{
			Matcher mymatch = aidxpath.matcher(onefile.getName());
			Util.massert(mymatch.find(), "Could not find pattern");
			
			String listcode = mymatch.group(1);
			
			Util.massert(ListInfoManager.getSing().havePosRequest(listcode),
				"Ready list code %s found but not entered in ListManager", listcode);
			
			rset.add(ListInfoManager.getSing().getPosRequest(listcode));
		}
		
		return rset;
	}
	
	public static File getListSaveDir(String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);
		Util.massert(UserIndexUtil.isBlockEndDay(daycode),
			"List bag dirs only available at block end, daycode is %s", daycode);
		
		return new File(Util.sprintf("%s/listsave/%s", LOCAL_UINDEX_DIR, daycode));
		
	}
	
	public static String getLocalListPath(String daycode, String listcode)
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid day code %s", daycode);
		Util.massert(isCanonicalDay(daycode), "Daycode %s is not canonical block end day", daycode);
		
		String datacenter = ListInfoManager.getSing().getDataCenterForList(listcode);
		return Util.sprintf("%s/%s/AIM_Index_%s__%s.list", 
			getListSaveDir(daycode).getAbsolutePath(), datacenter, listcode, daycode);
	}
	
	public static String getUserFeaturePath(String posPix, String negPix, String dayCode)
	{
		return Util.sprintf("%s/feature_reports/%s/feature_report_%sx%s.csv", 
			LOCAL_UINDEX_DIR, dayCode, posPix, negPix);
	}		
	
	public static String get3PartyFeaturePath(PosRequest posreq, String daycode)
	{
		UserIndexUtil.assertValidBlockEnd(daycode);

		return Util.sprintf("%s/feature_reports/%s/party3_report_%sx%s.csv", 
			LOCAL_UINDEX_DIR, daycode, posreq.getListCode(), posreq.getNegRequest().getListCode());
	}			

	// TODO: change to PosRequest object
	public static String getAdaUsedFeaturePath(String posPix, String negPix, String dayCode)
	{
		return Util.sprintf("%s/ada_used/%s/feature_weight_%sx%s.csv", 
			LOCAL_UINDEX_DIR, dayCode, posPix, negPix);
	}				
		
	public static String getHdfsAdaClassDir(String canday)
	{
		assertValidBlockEnd(canday);
		return Util.sprintf("/userindex/adaclass/%s", canday);
	}	
	
	public static String getHdfsSlicePath(ScanRequest screq, String daycode)
	{
		return getHdfsSlicePath(daycode, screq.getListCode());
	}
	
	@Deprecated 
	public static String getHdfsSlicePath(String daycode, String listcode)
	{
		return Util.sprintf("/userindex/dbslice/%s/%s.%s", daycode, listcode, SLICE_SUFF);
	}
		
	public static String getAdaSavePath(String listcode, String blockend)
	{
		return Util.sprintf("%s/saveinfo_%s.txt", getHdfsAdaClassDir(blockend), listcode);
	}
	
	public static String getStagingManifestPath(String blockstart)
	{
		Util.massert(isBlockStartDay(blockstart), 
			"Invalid block start day %s", blockstart);
		
		return Util.sprintf("/userindex/staging/%s/MANIFEST.txt", blockstart);
	}
	
	/*
	public static String getAdaAlphaPath(String listcode, String canday)
	{
		return Util.sprintf("%s/Alpha_%s.ser", getHdfsAdaClassDir(canday), listcode);
	}
	
	public static String getAdaFuncsPath(String listcode, String canday)
	{
		return Util.sprintf("%s/Funcs_%s.ser", getHdfsAdaClassDir(canday), listcode);
	}	
	*/
	
	public static String getTopDomainPath(CountryCode ccode, String daycode)
	{
		return Util.sprintf("%s/domaincount/%s/dc_%s.tsv", 
			LOCAL_UINDEX_DIR, daycode, ccode.toString().toUpperCase());
	}
	
	public static String getReadyMapPath(String blockend)
	{
		assertValidBlockEnd(blockend);
		String blockstart = getBlockStartForDay(blockend);
		return Util.sprintf("%s/%s/READY_MAP.txt", STAGING_DIR, blockstart);		
	}
	
	public static String getPrecompHdfsDir(String daycode)
	{
		return Util.sprintf("/userindex/precomp/%s", daycode);
	}
	
	public static String getPrecompHdfsPath(String daycode, ScanRequest scanreq)
	{
		return Util.sprintf("%s/%s.prec", getPrecompHdfsDir(daycode), scanreq.getListCode());
	}		
	
	public static int funcCode(String funcname)
	{
		int h = funcname.hashCode();
		h = (h < 0 ? -h : h);
		int k = (h % 10000);
		return k;
	}
	
	/*
	static void writeAdaToHdfs(FileSystem fsys, String listcode, AdaBoost<UserPack> adaclass, String canday)
	{
		String alphapath = Util.sprintf("%s/Alpha_%s.ser", getHdfsAdaClassDir(canday), listcode);
		String funcspath = Util.sprintf("%s/Funcs_%s.ser", getHdfsAdaClassDir(canday), listcode);
		
		try {
			HadoopUtil.deleteIfPresent(fsys, alphapath);
			HadoopUtil.deleteIfPresent(fsys, funcspath);
		} catch (IOException ioex) {
			throw new RuntimeException(ioex);	
		}
		
		HadoopUtil.serializeEat(adaclass.alpha, fsys, alphapath);
		HadoopUtil.serializeEat(adaclass.funcs, fsys, funcspath);
	}
	*/
	
	// Send each batch of pixel information to its own partition
	public static class SmartPartitioner implements Partitioner<Text, Text>
	{
		Map<String, Integer> _readyMap;
		String _blockEnd;
		
		Map<String, Integer> _oldReadyMap;
		
		
		@Override
		public void configure(JobConf jobconf) {
			
			_blockEnd = jobconf.get(HadoopJobInfo.blockend.toString());
		
			// codemap = UserIndexUtil.getListenCodeMap(jobconf);
			//Util.pf("\nFound listen code map %s", codemap);
		}
		
		public int getPartition(Text key, Text value, int numPart)
		{
			// This is a very lightweight call, just read a single text file
			if(_readyMap == null)
			{ 
				assertValidBlockEnd(_blockEnd);
				_readyMap = CheckReady.loadReadyMap(_blockEnd);
				_oldReadyMap = Util.treemap();
				
				for(String oneready : _readyMap.keySet())
				{
					Util.massert(ListInfoManager.getSing().haveRequest(oneready),
						"No list info found for listcode %s", oneready);
					
					ScanRequest screq = ListInfoManager.getSing().getRequest(oneready);
					
					_oldReadyMap.put(screq.getOldListCode(), _readyMap.get(oneready));
				}
			}
		
			String listcode = key.toString().split(Util.DUMB_SEP)[0];
			
			{
				Integer try_a = _readyMap.get(listcode);	
				if(try_a != null)  { return try_a; }
			}
			
			{
				Integer try_b = _oldReadyMap.get(listcode);				
				if(try_b != null)  { return try_b; }
			}
			
			Util.massert(false,  "No entry found in ready map for listcode %s", listcode);
			return -1;
		}
	}

	public static String getCanonicalEndDaycode()
	{
		return TimeUtil.cal2DayCode(getCanonicalEndDay());	
	}
	
	public static Calendar getCanonicalEndDay()
	{
		Calendar endday = TimeUtil.getYesterday(); // Never start today, data will never be available
		// SimpleDateFormat sdf = new SimpleDateFormat("EEEEEEE, MMMMMMMM dd, yyyy");
		while(!isCanonicalDay(endday))
		{
			endday = TimeUtil.dayBefore(endday);
		}

		// Util.pf("Day is %s\n", sdf.format(endday.getTime()));
		return endday;
	}
	
	public static void assertValidBlockEnd(String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);
		Util.massert(isBlockEndDay(daycode),
			"Day code %s is not a block-end date", daycode);
	}
	
	public static void assertValidBlockStart(String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);
		Util.massert(isBlockStartDay(daycode),
			"Day code %s is not a block-start date", daycode);
	}	
	
	
	public static String getNextBlockEnd(String daycode)
	{
		daycode = TimeUtil.dayAfter(daycode);
		
		while(!isBlockEndDay(daycode))
			{ daycode = TimeUtil.dayAfter(daycode); }
		
		return daycode;
	}
	
	public static boolean isCanonicalDay(String daycode)
	{
		return isCanonicalDay(TimeUtil.dayCode2Cal(daycode));
	}
	
	public static boolean isCanonicalDay(Calendar targcal)
	{
		return targcal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY;
	}
	
	public static boolean isBlockEndDay(String targday)
	{
		return isCanonicalDay(targday);
	}
	
	public static boolean isBlockEndDay(Calendar targcal)
	{
		return isCanonicalDay(targcal);
	}	
	
	public static boolean isBlockStartDay(String targday)
	{
		return isCanonicalDay(TimeUtil.dayBefore(targday));	
	}
	
	public static String getBlockStartForDay(String targday)
	{
		for(int i  : Util.range(9))
		{
			if(isBlockStartDay(targday))
				{ return targday; }
			
			targday = TimeUtil.dayBefore(targday);
		}
		
		throw new RuntimeException("Failed to find a good block start for " + targday);		
	}
	
	public static String getNextStartDay()
	{ return getNextStartDay(TimeUtil.getTodayCode()); }
	
	// The next scan start date after the given entry date.
	public static String getNextStartDay(String entrydate)
	{
		String gimp = entrydate;
		
		for( ; !isBlockStartDay(gimp); gimp = TimeUtil.dayAfter(gimp)) ;
		
		return gimp;
	}
		
	public static List<String> getCanonicalDayList(String blockend)
	{
		Util.massert(isCanonicalDay(blockend), "Must call with block end day");
		List<String> daylist = Util.vector();
		
		String curday = blockend;
		while(true)
		{
			daylist.add(curday);
			if(isBlockStartDay(curday))
				{ break; }
			
			curday = TimeUtil.dayBefore(curday);
		}
		
		Util.massert(daylist.size() == 7, "Expected daylist of size 7, found %d", daylist.size());
		return daylist;
	}
	
	
	public static int uniformWtpPartition(String wtp)
	{
		int hc = wtp.hashCode();
		hc = (hc < 0 ? -hc : hc); // Otherwise we'll generate negative partition data
		return (hc % NUM_SHUF_PARTITIONS);			
	}
	
	static Map<String, String> getPos2NegMap()
	{
		return ListInfoManager.getSing().getPos2NegMap();
	}
	
	static String getNegPoolPath(String daycode, CountryCode ccode)
	{
		return Util.sprintf("/userindex/negpools/%s/pool_%s.txt", daycode, ccode);
	}
	
	static String getLocalNegPoolPath(String daycode, String ccode)
	{
		return Util.sprintf("%s/negpools/%s/pool_%s.txt", LOCAL_UINDEX_DIR, daycode, ccode);
	}
	
	static void transformFile(List<BufferedWriter> writelist, String path) throws IOException
	{
		double startup = Util.curtime();
		int lcount = 0;
		
		BufferedReader bread = Util.getReader(path);
		
		for(String logline = bread.readLine(); logline != null; logline = bread.readLine())
		{
			String[] toks = logline.split("\t");
			
			String wtp = toks[0];
			
			int hc = uniformWtpPartition(wtp);
			
			// Util.pf("WTP is %s, hashcode is %d\n", wtp, hc);
			
			writelist.get(hc).write(logline + "\n");
			lcount++;
			
			if((lcount % 300000) == 0)
			{
				Util.pf("Wrote line %d, took %.03f seconds\n", lcount, (Util.curtime()-startup)/1000);
			}
		}
		
		Util.pf("Finished transforming file %s, took %.03f seconds\n", path, (Util.curtime()-startup)/1000);
	}
	
	public static Map<String, AdaBoost<UserPack>> readClassifData(FileSystem fsys, String canday) throws IOException
	{
		Map<String, AdaBoost<UserPack>> pix2ada = Util.treemap();
		String pathpatt = Util.sprintf("/userindex/adaclass/%s/saveinfo_*.txt", canday);
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, pathpatt);
		
		for(Path alphaname : pathlist)
		{
			//Util.pf("Found alphaname %s\n", alphaname);
			
			String listcode = alphaname.getName();
			listcode = listcode.substring("saveinfo_".length());
			listcode = listcode.substring(0, listcode.length()-4);
			
			//Util.pf("Found listcode %s\n", listcode);
			
			List<String> saveinfolist = HadoopUtil.readFileLinesE(alphaname);
			AdaBoost<UserPack> testada = new AdaBoost<UserPack>();

			testada.readSaveListInfo(saveinfolist);
			
			//Util.pf("\nFound %d functions, %d alpha values for list code %s", 
			//		testada.alpha.size(), testada.funcs.size(), listcode);
			
			pix2ada.put(listcode, testada);					
		}
		
		return pix2ada;		
	}
	
	
	static List<Pair<String, String>> getPixParamList(PxprmRequest pxreq)
	{
		return UserIdxDb.getPrmpxKeyVal(pxreq.getListCode());	
	}
	
	public static void main(String[] args)
	{
		getReadyListSet("2013-02-10");
	}
}
