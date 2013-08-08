
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;

import java.math.BigInteger;

public class FlatAcquireWebScan
{		
	public enum AcquireEnum { TargetListLoads, ListSizeTotal };
	
	public enum IpScanTarget { hospital, SpecialNeedHP, TestSpecHP, HP_200_to_999, HPCompanyMatched };
		
	public static final String SINGLE_RECORD_PATH = "/home/burfoot/acquireweb/SINGLE_RECORD_IMP_v19.txt";

	private String _dayCode; 
	
	private static String FTP_HOST_NAME = "ftp.lscaccess1.net";
	private static String FTP_USER_NAME = "digilant_OUT";
	private static String FTP_PASS_WORD = "digLABAS123";
		
	private List<String> _pathList;
	
	private List<ScanListener> _scanList = Util.vector();
	
	private SimpleMail _logMail;
	
	private static String[] _extraRecip = new String[] { 
		"charlie.tarzian@gmail.com", "jgiraldi@yahoo.com", "timothy.flink@digilant.com"
	
	};
	
	public static void main(String[] args) throws Exception
	{ 
		if(args.length != 1)
		{
			Util.pf("Usage FlatAcquireWebScan <yest|daycode>\n");
			return;
		}
		
		String daycode = "yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0];
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid day code: %s", daycode);
		
		FlatAcquireWebScan faws = new FlatAcquireWebScan(daycode);
		
		{
			LSCQuery lscq = new LSCQuery();
			lscq.initFromDb();
			
			long[] accttarg = new long[] { 739, 1852 };
			
			Map<BigInteger, String> campidmap = Util.treemap();
			
			for(long oneacct : accttarg)
			{
				for(BigInteger onecamp : lscq.camps4Acct(oneacct))
				{ 
					String campname = lscq.getCampaignName(onecamp);
					campidmap.put(onecamp, campname);
				}			
			}	
			
			faws.addCampListener(campidmap);
		}
		
		
		faws.createRemoteDirs();
		faws.runScan();
		
		faws._logMail.sendPlusAdmin(_extraRecip);
	}
	
	public FlatAcquireWebScan(String dc) throws IOException
	{
		_dayCode = dc;
		
		_logMail = new SimpleMail("AcquireWebScan for " + dc);
		
		_pathList = getNfsPathList(true, true);
		Collections.shuffle(_pathList);
		
		// _scanList.add(new FullScanListener());
		_scanList.add(new IpppScanListener(IpScanTarget.HP_200_to_999));
		_scanList.add(new IpppScanListener(IpScanTarget.HPCompanyMatched));
	}
	
	private void createRemoteDirs()
	{
		for(ScanListener onescan : _scanList)
		{
			createFtpDir(_dayCode, onescan.getListCode(), _logMail);
		}
	}
	
	void addCampListener(Map<BigInteger, String> campidmap)
	{
		CampIdScanListener campscan = new CampIdScanListener(campidmap.keySet());
		_scanList.add(campscan);
		
		for(BigInteger campid : campidmap.keySet())
		{
			_logMail.pf("Adding scan for campaign ID %s (%d)\n",
				campidmap.get(campid), campid);		
		}
	}
	
	
	private void runScan() throws IOException
	{		
		double startup = Util.curtime();
		
		for(int pid : Util.range(_pathList.size()))
		{
			String onepath = _pathList.get(pid);			
			// Util.pf("Starting scan of file %s\n", onepath);
			processFile(onepath);
			
			if((pid % 100) == 0)
			{
				double timesecs = (Util.curtime() - startup)/1000;
				double avgtime = timesecs/(pid+1);
				_logMail.pf("Completed path %d/%d, average time=%.03f, %s, estcomplete=%s\n", 
					pid, _pathList.size(), avgtime, getScanHitCount(),
					TimeUtil.getEstCompletedTime(pid+1, _pathList.size(), startup));
				
				for(ScanListener onescan : _scanList)
				{
					_logMail.pf("%s :: %s\n", onescan.getListCode(), onescan.getStatusInfo());
				}
			}
		}
		
		for(ScanListener onescan : _scanList)
			{ onescan.finishScan(); }
	}
	
	private List<String> getNfsPathList(boolean useimp, boolean usebid)
	{
		List<String> pathlist = Util.vector();
		
		List<LogType> ltype = Util.vector();
		ltype.add(LogType.conversion);
		ltype.add(LogType.click);
		
		if(useimp)
			ltype.add(LogType.imp);
		
		if(usebid)
			ltype.add(LogType.bid_all);
		
		
		for(LogType lt : ltype)
		{
			for(ExcName ename : ExcName.values())
			{
				List<String> onelist = Util.getNfsLogPaths(ename, lt, _dayCode);
				if(onelist != null)
					{ pathlist.addAll(onelist); }
			}
		}
		return pathlist;
	}
	
	private void processFile(String nfspath) throws IOException
	{
		PathInfo pinf = new PathInfo(nfspath);
		
		BufferedReader bread = FileUtils.getGzipReader(nfspath);
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			// Util.pf("Scanning one line %s\n", oneline);
			
			BidLogEntry ble = BidLogEntry.getOrNull(pinf.pType, pinf.pVers, oneline);
			if(ble == null)
				{ continue; }
			
			for(ScanListener onescan : _scanList)
				{ onescan.present(ble); }

		}
		bread.close();
	}
	
	private static String getIpTargetList(IpScanTarget ist)
	{
		return Util.sprintf("/local/fellowship/acquireweb/lserve/iplist/%s.txt", ist);
	}
	
	static void createFtpDir(String daycode, String scancode, SimpleMail lmail)
	{
		JschFtpUtil jfu = new JschFtpUtil(FTP_HOST_NAME, FTP_USER_NAME, FTP_PASS_WORD);
		String ftpdir = getFtpBatchDir(daycode, scancode);
		lmail.pf("Attempting to create remote directory %s\n", ftpdir);
		boolean didcreate = jfu.createDir(ftpdir);
		
		if(didcreate)
			{ lmail.pf("Created remote directory %s\n", ftpdir);	}
		else 
			{ lmail.pf("Remote directory %s already exists\n", ftpdir); }
	}
	
	static void transferBatchFile(String daycode, String listcode, int batchid, SimpleMail lmail)
	{
		// /local/fellowship/acquireweb/ftproot/output/2012-12-06/SpecialNeedHP
		
		JschFtpUtil jfu = new JschFtpUtil(FTP_HOST_NAME, FTP_USER_NAME, FTP_PASS_WORD);
		File localbatch = new File(getFullBatchPath(daycode, listcode, batchid));
		String ftpsuffpath = getFtpBatchPath(daycode, listcode, batchid);
		jfu.putFile(localbatch, ftpsuffpath);
		lmail.pf("Finished uploading file %s\n", ftpsuffpath);
		
		// Cleaning up
		localbatch.delete();
	}
	
	// ListCode --> Number of hits
	private Map<String, Integer> getScanHitCount()
	{
		Map<String, Integer> hitmap = Util.treemap();
		
		for(ScanListener slist : _scanList)
			{ hitmap.put(slist.getListCode(), slist.hitCount); }
		
		return hitmap;
	}
	
	public static String getFullBatchPath(String daycode, String listcode, int batchid)
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid Day Code %s", daycode);
		return "/local/fellowship/acquireweb/ftproot" + getFtpBatchPath(daycode, listcode, batchid);
	}
	
	public static String getFtpBatchPath(String daycode, String listcode, int batchid)
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid Day Code %s", daycode);
		return Util.sprintf("%s/batch_%s_%s.txt.gz", getFtpBatchDir(daycode, listcode), 
			Util.padLeadingZeros(""+batchid, 4), daycode);
	}
	
	public static String getFtpBatchDir(String daycode, String listcode)
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid Day Code %s", daycode);
		return Util.sprintf("/output/%s/%s", listcode, daycode);
	}
	
	private abstract class ScanListener 
	{
		protected int _curLineCount = 0;
		protected int _curBatchId = 0;
		
		protected int hitCount = 0;
		
		// public static final int RECORDS_PER_BATCH = 10000;
		
		public static final int RECORDS_PER_BATCH = 100000;
		
		public static final int NUM_BATCHES = 100;		
		
		BufferedWriter _bWrite;
		
		// These are the fields we want
		protected Set<LogField> _targFieldSet = Util.treeset(); 
		
		// "Code" for scanlistener
		public abstract String getScanCode();
		
		public abstract String getListCode();
		
		public abstract String getStatusInfo();
		
		// True if the listener is interested in this record
		public abstract boolean accept(BidLogEntry ble);
		
		
		public ScanListener()
		{
			initTargFields();
		}
		
		public void present(BidLogEntry ble) throws IOException
		{
			if(accept(ble))
			{
				hitCount++;
				openIfNecessary();
				
				for(LogField fname : _targFieldSet)
				{
					_bWrite.write(ble.getField(fname));
					_bWrite.write("\t");
				}
				
				finishLine(ble.getLogType());
			}
		}
		
		protected void initTargFields()
		{
			List<String> singlerec = FileUtils.readFileLinesE(SINGLE_RECORD_PATH);
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v19, singlerec.get(0));
			
			// This is paranoid error-checking
			for(String onefield : FileUtils.readFileLinesE(getTargFieldPath()))
			{
				LogField fname = LogField.valueOf(onefield.trim());
				Util.massert(ble.hasField(fname), "Fname not found %s", fname);
				_targFieldSet.add(fname);
			}
			
			_logMail.pf("Found %d target fields: %s\n", _targFieldSet.size(), _targFieldSet);
		}		
		
		private String getTargFieldPath()
		{
			return Util.sprintf("/local/fellowship/acquireweb/targfields_%s.txt", getScanCode());
		}
		
		private void openIfNecessary() throws IOException
		{
			if(_bWrite == null)
			{	
				String batchpath = getFullBatchPath(_dayCode, getListCode(), _curBatchId);
				FileUtils.createDirForPath(batchpath);
				_bWrite = FileUtils.getGzipWriter(batchpath);
				
				// Initialize headers
				for(LogField fname : _targFieldSet)
				{ 
					_bWrite.write(fname.toString());
					_bWrite.write("\t");
				} 
				_bWrite.write("logtype");
				_bWrite.write("\n");
			}
		}		
		
		
		public void finishScan() throws IOException
		{
			closeNUpload();
		}
		
		private void closeNUpload() throws IOException
		{
			if(_bWrite != null)
			{
				_bWrite.close();
				_bWrite = null;		
			}
			
			transferBatchFile(_dayCode, getListCode(), _curBatchId, _logMail);	
			
		}
		
		private void finishLine(LogType ltype) throws IOException
		{
			_bWrite.write(ltype.toString());
			_bWrite.write("\n");
			
			_curLineCount++;
			
			if(_curLineCount == RECORDS_PER_BATCH)
			{
				// _logMail.pf("Finished for %s, batchid=%d\n", getListCode(), _curBatchId);
				closeNUpload();
				
				_curBatchId++;
				_curLineCount = 0;
			}
		}		
	}
	

	
	
	private class IpppScanListener extends ScanListener
	{
		private TreeSet<Long> _targIpSet = Util.treeset();
		
		private IpScanTarget _ipTarget;
		
		private int _maxUser; 
		
		private int _hitCount = 0;
				
		public IpppScanListener(IpScanTarget ist) throws IOException
		{
			this(ist, Integer.MAX_VALUE);
		}
		
		public IpppScanListener(IpScanTarget ist, int maxu) throws IOException
		{
			_maxUser = maxu;
			_ipTarget = ist;
			loadTargetsFromTxt();
		}		
		
		public String getListCode()
		{
			return _ipTarget.toString();	
		}
		
		public String getScanCode()
		{
			return "ipppscan";
		}
		
		public boolean accept(BidLogEntry ble)
		{
			String ipstr = ble.getField(LogField.user_ip);
			Long ipaddr = null;
			try { 
				ipaddr = Util.ip2long(ipstr); 
				_hitCount += (_targIpSet.contains(ipaddr) ? 1 : 0);
				return _targIpSet.contains(ipaddr);
			} 
			catch (Exception ex) { }
				
			return false;			
		}
		
		private void loadTargetsFromTxt() throws IOException
		{
			double startup = Util.curtime();
			_logMail.pf("Loading target set from file %s\n", getIpTargetList(_ipTarget));
			BufferedReader bread = FileUtils.getReader(getIpTargetList(_ipTarget));
			
			bread.readLine(); // discard first
			
			for(String oneline = bread.readLine(); oneline != null ; oneline = bread.readLine())
			{
				Long iplong = Util.ip2long(oneline.trim());
				_targIpSet.add(iplong);
				
				if(_targIpSet.size() >= _maxUser)
					{ break; }
			}
			bread.close();
			
			_logMail.pf("loaded target set %s, found %d targets, took %.03f secs\n", 
				_ipTarget.toString().toUpperCase(), _targIpSet.size(), (Util.curtime()-startup)/1000);
		}		
		
		public String getStatusInfo()
		{
			return Util.sprintf("HitCount: %d", _hitCount);	
		}
	}
	
	/*
	private class FullScanListener extends ScanListener
	{	
		public String getScanCode()
		{
			return "fullscan";
		}		
		
		public String getListCode()
		{
			return "fulldata";	
		}
		
		public boolean accept(BidLogEntry ble)
		{
			return true;	
		}
	}
	*/
	
	private class CampIdScanListener extends ScanListener
	{	
		TreeMap<Integer, Integer> _campTargMap = Util.treemap();
		
		public CampIdScanListener(Collection<? extends Number> camptarg)
		{
			for(Number oneid : camptarg)
				{  _campTargMap.put(oneid.intValue(), 0); }
		}
		
		public String getScanCode()
		{
			return "campscan";
		}		
		
		public String getListCode()
		{
			return Util.sprintf("campscan");
		}
		
		public boolean accept(BidLogEntry ble)
		{
			int campid = ble.getIntField(LogField.campaign_id);
			
			if(_campTargMap.containsKey(campid))
			{
				// Util.pf("Found a hit for campid=%d\n", campid);
				Util.incHitMap(_campTargMap, campid);
				return true;
			}
			
			return false;
		}
		
		public String getStatusInfo()
		{
			return Util.sprintf("Hits4Campaign: %s", _campTargMap);
			
		}
	}	
	
}
