/**
 * 
 */
package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


public class ClickChecker 
{	
	private static int WTP_HIT_CUTOFF = 10;
	private static int IP_HIT_CUTOFF = 10;

	private static final LogField COOKIE_FIELD_NAME = LogField.uuid;

	private Map<WtpId, Integer> _uuidCountMap = Util.treemap();
	private Map<Long, Integer> _ipCountMap = Util.treemap();

	private Set<String> _auctionIdSet = Util.treeset();	

	private List<BidLogEntry> _newLogList = Util.vector();

	private File _logSaveDir = null;
	private Integer _saveBatchId = null;

	public ClickChecker(File lsd)
	{
		Util.massert(lsd.exists() && lsd.isDirectory(),
			"Error: directory %s does not exist or is not a directory", lsd);

		_logSaveDir = lsd;
		_saveBatchId = 0;

		rebuildFromLog();
	}

	public ClickChecker()
	{


	}

	// This function is pure/immutable
	public synchronized boolean greenLight(BidLogEntry ble)
	{
		Util.massert(ble.getLogType() == LogType.click,
			"Attempt to use click checker on BLE with type %s", ble.getLogType());

		return (auctionOkay(ble) && ipOkay(ble) && wtpOkay(ble));
	}

	private boolean wtpOkay(BidLogEntry ble)
	{
		// TODO: should this be wtp_user_id or uuid?
		String wtpid = ble.getField(COOKIE_FIELD_NAME);

		// TODO: do we really just return true here? 
		WtpId wid = WtpId.getOrNull(wtpid);
		if(wid == null)		
			{ return true; }

		Integer count = _uuidCountMap.get(wid);
		return (count == null || count < WTP_HIT_CUTOFF);
	}

	private boolean ipOkay(BidLogEntry ble)
	{
		String ipstr = ble.getField(LogField.user_ip).trim();

		Long ip = null;
		try { ip = Util.ip2long(ipstr); }
		catch (IllegalArgumentException illex) { return false; }

		Integer count = _ipCountMap.get(ip);
		return (count == null || count < 10);
	}

	private boolean auctionOkay(BidLogEntry ble)
	{
		String auctid = ble.getField(LogField.auction_id);
		return !_auctionIdSet.contains(auctid);
	}

	public synchronized void  add2dataSet(BidLogEntry ble)
	{
		try { 
			Long ip = Util.ip2long(ble.getField(LogField.user_ip).trim());
			Util.incHitMap(_ipCountMap, ip);
		} catch (IllegalArgumentException illex) { 
			return;
		}

		// Util.pf("Auction ID: %s\n", ble.getField("auction_id"));

		String auctid = ble.getField(LogField.auction_id);
		_auctionIdSet.add(auctid);

		WtpId wid = WtpId.getOrNull(ble.getField(COOKIE_FIELD_NAME));
		if(wid == null)
			{  wid = WtpId.getOrNull(Util.WTP_ZERO_ID); }

		_newLogList.add(ble);
	}


	private void rebuildFromLog()
	{
		Util.massert(_logSaveDir != null && _saveBatchId != null && _saveBatchId == 0, 
			"Flush data called with no LogSaveDir, or error with save batch ID");

		for( ; ; _saveBatchId++)
		{
			File batchfile = new File(getCurBatchSavePath());
			if(!batchfile.exists())
				{ break; }

			List<String> batchlist = FileUtils.readFileLinesE(batchfile.getAbsolutePath());

			for(String onerec : batchlist)
			{
				String[] ts_aid_ip_wtp = onerec.split("\t");

				String auctid = ts_aid_ip_wtp[1];
				_auctionIdSet.add(auctid);			

				Long ip = Util.ip2long(ts_aid_ip_wtp[2]);
				Util.incHitMap(_ipCountMap, ip);

				WtpId wid = WtpId.getOrNull(ts_aid_ip_wtp[3]);
				if(wid != null)
					{  Util.incHitMap(_uuidCountMap, wid); }
			}
		}

		Util.pf("ClickChecker::loaded %d save batch files from logsavedir=%s\n",
			_saveBatchId, _logSaveDir.getAbsolutePath());

	}

	public void flushData()
	{
		Util.massert(_logSaveDir != null && _saveBatchId != null, "Flush data called with no LogSaveDir");

		List<String> writelist = Util.vector();

		for(BidLogEntry ble : _newLogList)
		{
			// Order is timestamp, auction, ip, wtp
			// Want this because auction and wtp often have same format, so it's good to separate them.
			WtpId wid = WtpId.getOrNull(ble.getField(COOKIE_FIELD_NAME).trim());
			if(wid == null)
				{ wid = WtpId.getOrNull(Util.WTP_ZERO_ID); }

			String oneline = Util.varjoin("\t", 
				ble.getField(LogField.date_time), ble.getField(LogField.auction_id), 
				ble.getField(LogField.user_ip), wid.toString() );

			writelist.add(oneline);
		}

		FileUtils.writeFileLinesE(writelist, getCurBatchSavePath());
		Util.pf("ClickChecker:: saved %d new click records for batch %d\n", writelist.size(), _saveBatchId);

		_saveBatchId++;
		_newLogList.clear();
	}

	private String getCurBatchSavePath()
	{
		return Util.sprintf("%s/clickbatch__%s.txt", 
			_logSaveDir.getAbsolutePath(),	Util.padLeadingZeros(_saveBatchId, 5));
	}

	public static class DailyClickCleaner
	{
		SimpleMail _logMail;
		String _dayCode;
		
		LogVersion _minVers;
		ClickChecker _cCheck; 
		
		List<String> _pathList = Util.vector();
		
		BufferedWriter _dayWriter; 
		
		int _okayRecs = 0, _badRecs = 0, _corruptRecs = 0;
		
		int _maxFile = 0;
		
		private DailyClickCleaner(String daycode, int mxf)
		{
			TimeUtil.assertValidDayCode(daycode);
			
			_dayCode = daycode;
			_logMail = new DayLogMail(this, daycode);
			_cCheck = new ClickChecker();
			
			_maxFile = mxf;
		}
		
		void fullProcess()  throws IOException
		{
			initPathList();
			runScan();
			upload2hdfs();
			
			_logMail.send2admin();
		}
		
		void initPathList() throws IOException
		{
			for(ExcName exc : ExcName.values())
			{
				List<String> plist = Util.getNfsLogPaths(exc, LogType.click, _dayCode);
				
				if(plist != null)
					{ _pathList.addAll(plist); }
			}
			
			Util.massert(!_pathList.isEmpty(), "Found no paths");
			
			// Find the log version
			{
				SortedSet<String> verset = Util.treeset();
				
				for(String onepath : _pathList)
				{
					PathInfo pinfo = new PathInfo(onepath);
					verset.add(pinfo.pVers.toString());
				}
				
				_minVers = LogVersion.valueOf(verset.first());
				_logMail.pf("Found version-set %s, using %s\n", 
					verset, _minVers);
			}
			
			_logMail.pf("Found %d total click files\n", _pathList.size());
		}
		
		void runScan() throws IOException
		{
			_dayWriter = FileUtils.getGzipWriter(getLocalFilterPath(_dayCode, _minVers));			
			
			for(int i = 1; i <= _pathList.size(); i++)
			{
				String onepath = _pathList.get(i-1);
				
				processFile(onepath);
				
				if((i % 500) == 0)
				{
					_logMail.pf("Finished with file %d / %d, found %d bad and %d okay records so far\n", 
						i, _pathList.size(), _badRecs, _okayRecs);
				}
				
				if(i >= _maxFile)
				{
					_logMail.pf("Hit max-file limit, breaking\n");
					break;
				}
			}
			
			_dayWriter.close();
		}
		
		void processFile(String onepath)  throws IOException
		{
			PathInfo pinfo = new PathInfo(onepath);
			BufferedReader bread = FileUtils.getGzipReader(onepath);
			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
				if(ble == null)
				{ 
					_corruptRecs++; 
					continue;
				}
				
				if(_cCheck.greenLight(ble))
				{
					_dayWriter.write(oneline);
					_dayWriter.write("\n");
					_okayRecs++;
					
				} else {
					
					_badRecs++;	
				}
				
				_cCheck.add2dataSet(ble);
			}
			
			bread.close();
		}
		
		void upload2hdfs()  throws IOException
		{
			FileSystem fsys = FileSystem.get(new Configuration());
			
			Path haddst = new Path(Util.sprintf("/data/cleanclick/allexc_%s_%s.log.gz", _dayCode, _minVers));
			
			if(fsys.exists(haddst))
			{
				_logMail.pf("HDFS path %s exists, deleting\n", haddst);	
				fsys.delete(haddst, false);
			}
			
			Path localsrc = new Path("file://" + getLocalFilterPath(_dayCode, _minVers));
			fsys.copyFromLocalFile(localsrc, haddst);
			
			_logMail.pf("Local src file %s\n", localsrc);
			_logMail.pf("Uploaded to HDFS %s\n", haddst);
		}
		
		public static String getLocalFilterPath(String daycode, LogVersion lvers)
		{
			return Util.sprintf("/local/fellowship/cleanclick/allexc_%s_%s.log.gz", daycode, lvers);
		}
	}
	
	// Run ClickChecker for a single day
	public static void main(String[] args) throws IOException
	{
		String daycode = args[0];
		daycode = "yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode;
		
		ArgMap argmap = Util.getClArgMap(args);
		int maxfile = argmap.getInt("maxfile", Integer.MAX_VALUE);
		
		DailyClickCleaner dcc = new DailyClickCleaner(daycode, maxfile);
		dcc.fullProcess();
	}
}
