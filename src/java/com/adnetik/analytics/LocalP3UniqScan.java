
package com.adnetik.analytics;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.data_management.*;
import com.adnetik.data_management.ExelateDataMan.*;
import com.adnetik.data_management.BluekaiDataMan.*;
import com.adnetik.analytics.ThirdPartyDataUploader.*;


public class LocalP3UniqScan
{
	SimpleMail _logMail;
	String _dayCode;
		
	List<String> _pathList = Util.vector();
		
	Set<LogType> _lTypeSet = Util.treeset();

	BufferedWriter _curWriter;
	
	Integer _maxFile;
	
	String _idPrefCutoff;
	
	int _errCount = 0;
	
	private Set<Part3Code> _partSet = Util.treeset();	
	
	public static void main(String[] args) throws Exception
	{
		String daycode = args[0];
		daycode = "yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode;
		daycode = "yestyest".equals(daycode) ? TimeUtil.dayBefore(TimeUtil.getYesterdayCode()) : daycode;
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid day code %s", daycode);

		ArgMap amap = Util.getClArgMap(args);
		
		LocalP3UniqScan uniqscan = new LocalP3UniqScan(daycode, amap);
		uniqscan.doProcess();
		
	}
	
	LocalP3UniqScan(String dc, ArgMap amap)
	{
		_dayCode = dc;
		
		_logMail = new DayLogMail(this, _dayCode);
		
		if(amap.getBoolean("useimp", true))
			{ _lTypeSet.add(LogType.imp); }
		
		if(amap.getBoolean("useclick", true))
			{ _lTypeSet.add(LogType.click); }
		
		if(amap.getBoolean("useconversion", true))
			{ _lTypeSet.add(LogType.conversion); }
		
		for(Part3Code p3c : Part3Code.values())
		{
			if(amap.getBoolean("use_" + p3c.toString(), true))
				{ _partSet.add(p3c); }
		}
				
		_maxFile = amap.getInt("maxfile", Integer.MAX_VALUE);
		
		// This is a comparison prefix we use to figure out if we should stop
		// By default, use ALL the data
		_idPrefCutoff = amap.getString("cutoff", "ZZZZZZ");
	}
	
	
	public static String getLocalDumpFile(String daycode)
	{
		return getLocalFile("dump", daycode);
	}
	
	public static String getLocalResultFile(String daycode)
	{
		return getLocalFile("result", daycode);
	}	
	
	private static String getLocalFile(String lfcode, String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);
		return Util.sprintf("/local/fellowship/thirdparty/uniqscan/%s_%s.txt", lfcode, daycode);
	}
	
	void initPathList()
	{
		for(ExcName oneexc : ExcName.values())
		{
			for(LogType ltype : _lTypeSet)
			{
				List<String> plist = Util.getNfsLogPaths(oneexc, ltype, _dayCode);
				
				if(plist != null)
					{ _pathList.addAll(plist); }
			}
		}
		
		Collections.shuffle(_pathList);
		
		_logMail.pf("Found %d NFS paths, using lt-set %s\n",
			_pathList.size(), _lTypeSet);
	}
	
	void doProcess() throws IOException
	{
		if(!checkMasterListData())
		{
			_logMail.pf("Missing 3rd Party Snapshot data, aborting\n");
			return;
		}
				
		initPathList();
		
		prepIccData();
		sortIccData();
		
		LocalMergeOp lmo = new LocalMergeOp();
		lmo.doMerge();
		
		ThirdPartyDataUploader tpdu = new ThirdPartyDataUploader(_dayCode);
		tpdu.initDataReader(getLocalResultFile(_dayCode));
		tpdu.runProcess();		
		
		// Clean up
		(new File(getLocalDumpFile(_dayCode))).delete();
		(new File(getLocalResultFile(_dayCode))).delete();
	}
	
	void prepIccData() throws IOException
	{
		int fcount = 0;
		double startup = Util.curtime();
		
		_curWriter = FileUtils.getWriter(getLocalDumpFile(_dayCode));
		
		for(String onepath : _pathList)
		{
			try { processOneFile(onepath); }
			catch (IOException ioex) {
				if(_errCount < 3)
				{
					_logMail.addExceptionData(ioex);
					_errCount++;
				}
			}
			
			fcount++;
			
			if(fcount > _maxFile)
				{ break; }
			
			if((fcount % 100) == 0)
			{
				double secsperfile = (Util.curtime() - startup)/1000;
				secsperfile /= fcount;
				
				_logMail.pf("Finished preprocessing file %d/%d, %.03f secs/file\n",
					fcount, _pathList.size(), secsperfile);
			}
		}
		
		_curWriter.close();
	}
	
	private boolean checkMasterListData() throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		List<Path> pathlist = Util.vector();
		
		if(_partSet.contains(Part3Code.BK)) 
		{ 
			Path bkmaster = new Path(BluekaiDataMan.BK_PATH_MAN.getHdfsMasterPath(_dayCode)); 
			pathlist.add(bkmaster);
		}
		
		if(_partSet.contains(Part3Code.EX)) 
		{ 	
			Path exmaster = new Path(ExelateDataMan.EX_PATH_MAN.getHdfsMasterPath(_dayCode)); 
			pathlist.add(exmaster);
		}
		
		for(Path onepath : pathlist)
		{
			if(!fsys.exists(onepath))
			{
				_logMail.pf("Master list path %s does not exist, aborting\n", onepath);	
				return false;
			}			
			
			_logMail.pf("Found snapshot file %s\n", onepath);
		}
		
		return true;		
	}
	
	void sortIccData() throws IOException
	{
		_logMail.pf("Preparing to sort dump file\n");
		Util.unixsort(getLocalDumpFile(_dayCode), "");
		_logMail.pf("Done");
	}
	
	private void processOneFile(String nfspath) throws IOException
	{
		PathInfo pinfo = new PathInfo(nfspath);
		
		BufferedReader bread = FileUtils.getGzipReader(nfspath);
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
			if(ble == null)
			{
				// TODO: log bad BLEs	
				continue;
			}
			
			WtpId wid = WtpId.getOrNull(ble.getField(LogField.wtp_user_id).trim());
			if(wid == null)
				{ return; }
			
			Integer campid = ble.getIntField(LogField.campaign_id);
			Integer lineid = ble.getIntField(LogField.line_item_id);
			String linetype = ble.getField(LogField.line_item_type);
			linetype = linetype.trim().length() == 0 ? "NOTSET" : linetype;		
			
			String combkey = Util.varjoin(Util.DUMB_SEP, wid, campid, lineid, linetype, pinfo.pType);
			FileUtils.writeRow(_curWriter, "\t", combkey, "1");			
		}
		
		bread.close();		
	}
	
	
	private class LocalMergeOp
	{
		private String _curWtp = null;
		private ExUserPack _curExPack = null;
		private BluserPack _curBkPack = null;
		
		private int _foundExCount = 0;
		private int _foundBkCount = 0;
		
		private int _totalUserCount = 0;
		
		BufferedWriter _resWriter;

		LocalMergeOp() throws IOException
		{
			if(_partSet.contains(Part3Code.EX))
				{ ExelateDataMan.setSingQ(_dayCode); }
			
			if(_partSet.contains(Part3Code.BK))
				{ BluekaiDataMan.setSingQ(_dayCode); }
			
			_resWriter = FileUtils.getWriter(getLocalResultFile(_dayCode));
		}
		
		void doMerge() throws IOException
		{
			// false=no sort
			SortedFileMap sortmap = SortedFileMap.buildFromFile(new File(getLocalDumpFile(_dayCode)), false);
			
			while(sortmap.hasNext())
			{
				Map.Entry<String, List<String>> myentry = sortmap.pollFirstEntry();
				
				String wtpid = mergeLine(myentry.getKey(), myentry.getValue());
				
				// Quit if the WTP ID of the line is above the cutoff
				if(wtpid.compareTo(_idPrefCutoff) > 0)
					{ break; }
			}
			
			sortmap.close();
			_resWriter.close();
		}
		
		String mergeLine(String key, List<String> vlist) throws IOException
		{
			// Ugh, big conjoined key string 
			String[] wtp_camp_line_linetype_lt = key.toString().split(Util.DUMB_SEP);
			
			LogType rectype = LogType.valueOf(wtp_camp_line_linetype_lt[4]);
			
			// Don't care 
			int hitcount = vlist.size();
			
			// Classic bug
			// Multiple calls to the Bk/ExDataMan.getSing().lookup(...) with the same ID
			// will return null after the first call.
			// So we have to remember the result of previous calls.
			// Possible I should change how the DataMan works, but this method will work either way.
			String newwtp = wtp_camp_line_linetype_lt[0];
			if(!newwtp.equals(_curWtp))
			{
				_curWtp = newwtp;
				
				if(_partSet.contains(Part3Code.EX))
				{ 
					_curExPack = ExelateDataMan.getSingQ().lookup(_curWtp); 
					
					if(_curExPack != null)
						{ _foundExCount++; }
				}
				
				if(_partSet.contains(Part3Code.BK))
				{
					_curBkPack = BluekaiDataMan.getSingQ().lookup(_curWtp); 
					
					if(_curBkPack != null)
						{ _foundBkCount++; }
				}
				
				_totalUserCount++;
				
				if((_totalUserCount % 100000) == 0)
				{
					_logMail.pf("Total user count is %d, found EX %d, found BK %d\n",
						_totalUserCount, _foundExCount, _foundBkCount);
				}
			}
			
			if(_curExPack != null)
			{
				String segstr = Util.join(_curExPack.getAllSegData(), ",");
				FileUtils.writeRow(_resWriter, "\t", key, "exelate", rectype, hitcount, segstr); 
			}
			
			if(_curBkPack != null)
			{
				String segstr = Util.join(_curBkPack.getAllSegData(), ",");
				FileUtils.writeRow(_resWriter, "\t", key, "bluekai", rectype, hitcount, segstr); 
			}		
			
			return _curWtp;
		}
		
		/*
		private String getOutputResult(String ptype, int hitcount, LogType ltype, Collection<Integer> segdata)
		{
			StringBuffer sb = new StringBuffer();
			sb.append(ptype);
			sb.append("\t");
			sb.append(ltype);
			sb.append("\t");
			sb.append(hitcount);
			sb.append("\t");
			sb.append(Util.join(segdata, ","));
			return sb.toString();	
		}		
		*/
	}
}

