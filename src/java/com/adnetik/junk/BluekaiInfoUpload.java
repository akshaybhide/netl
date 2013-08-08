
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import java.text.SimpleDateFormat;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.ExcName;

import com.adnetik.shared.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.analytics.HistogramTool;

import com.adnetik.data_management.BluekaiDataMan;

/** 
 *  Statistics about the slice data accumulated in the scanning process.
 */ 
public class BluekaiInfoUpload
{	
	
	public static int NULL_SEG_ID = -123123;
	private static int TARG_LIST_SIZE = 1000;
	
	private static final int ROWS_PER_BATCH = 100000;
	private static final int MAX_USERS_PER_LIST = 20000;
	
	public static final String BK_TABLE_NAME = "bluekai_data";
	
	private String _canDay;
	
	private Map<String, Integer> _reportIdMap = Util.treemap();
			
	private static final String LOCAL_STAGING_COPY = "/home/burfoot/userindex/bluekai/LOCAL_PIXEL_STAGE.txt";
	private static final String RESULT_LOCATION = "/home/burfoot/userindex/bluekai/RESULT_FILE.txt";
	
	private BufferedWriter _batchWriter = null;
	
	private int _batchCount = 0;
	private int _totalCount = 0;
	private int _usersCount = 0;
	
	private int _stageLinesRead = 0;
	
	private SimpleMail _logMail;
	
	public static void main(String[] args) throws Exception
	{
		BluekaiInfoUpload bkiu = new BluekaiInfoUpload();
		bkiu.fullOperation();
	}
	
	public BluekaiInfoUpload()
	{
		_canDay = UserIndexUtil.getCanonicalEndDaycode();
		
		_logMail = new SimpleMail("BluekaiInfoUpload Report for " + _canDay);
		
		_logMail.pf("Running BluekaiInfoUpload for canday=%s\n", _canDay);
	}
	
	private void fullOperation() throws IOException
	{
		grabStagingInfo();
		sortStagingInfo();
		runZipOperation();
		_logMail.send2admin();
	}
	
	private void sortStagingInfo() throws IOException
	{
		_logMail.pf("Beginning sort of staging info...");
		Util.unixsort(LOCAL_STAGING_COPY, LOCAL_STAGING_COPY + "~~", ""); 
		_logMail.pf(" done with sort");
	}
	
	private void grabStagingInfo() throws IOException
	{
		{
			File oldfile = new File(LOCAL_STAGING_COPY);
			if(oldfile.exists())
			{ 
				oldfile.delete(); 
				_logMail.pf("Deleted old version of local staging copy\n");
			}
		
		}
		
		_logMail.pf("Downloading local copy of staging data... ");
		FileSystem fsys = FileSystem.get(new Configuration());
		String blockstart = UserIndexUtil.getBlockStartForDay(_canDay);
		Path stagingpath = new Path(UserIndexUtil.getStagingInfoPath(StagingType.pixel, blockstart));
		fsys.copyToLocalFile(false, stagingpath, new Path("file://" + LOCAL_STAGING_COPY));
		
		_logMail.pf("Downloaded local copy of staging data, size is %d\n", (new File(LOCAL_STAGING_COPY)).length());
		
	}
	
	private int getReportId(String listcode)
	{
		if(!_reportIdMap.containsKey(listcode))
		{
			int newid = UserIdxDb.lookupCreateRepId(_canDay, listcode);
			_reportIdMap.put(listcode, newid);
		}
		
		return _reportIdMap.get(listcode);
	}
	
	private void deleteOld()
	{
		String sql = Util.sprintf("DELETE FROM %s WHERE report_id IN ( SELECT report_id FROM report_info WHERE can_day = '%s' )",
			BK_TABLE_NAME, _canDay);
		int delrows = DbUtil.execSqlUpdate(sql, new UserIdxDb());
		Util.pf("Deleted %d old rows of data\n", delrows);
	}
	
	
	private void runZipOperation() throws IOException
	{
		deleteOld();
		
		BluekaiDataMan.BlueUserQueue bqueue = BluekaiDataMan.getSingQ();
		SortStageList ssl = new SortStageList();
		
		while(ssl.hasNext())
		{
			Map.Entry<String, Collection<String>> stagepack = ssl.nextPack();	
			String wtpid = stagepack.getKey();
			_usersCount++;			
			
			BluekaiDataMan.BluserPack bpack = bqueue.lookup(wtpid);
						
			if((_usersCount % 10000) == 999)
				{ _logMail.pf("Finished user %d\n", _usersCount); }
			
			if(bpack == null)
			{
				// Util.pf("No Bluekai data found for WTP=%s\n", wtpid);
				for(String listcode : stagepack.getValue())
					{ writeLineInfo(listcode, wtpid, null, null); }
				
				continue; 
			}

			Map<Integer, String> segdata = bpack.getSegDataMap();
			// Util.pf("Found BluserPack with %d segments for wtp=%s\n", segdata.size(), wtpid);
			
			for(Integer segid : segdata.keySet())
			{
				String recency = segdata.get(segid);
				for(String listcode : stagepack.getValue())
					{ writeLineInfo(listcode, wtpid, recency, segid); }
			}
		}
				
		upload2db();
	}
	
	private void writeLineInfo(String listcode, String wtpid, String recency, Integer segid) throws IOException
	{
		int reportid = getReportId(listcode);
		String resline = Util.sprintf("%d\t%s\t%s\t%d\n", 
			reportid, wtpid, (recency == null ? "NULL" : recency), (segid == null ? NULL_SEG_ID : segid));

		if(_batchWriter == null)
			{ _batchWriter = FileUtils.getWriter(RESULT_LOCATION); }
		
		_batchWriter.write(resline);
		_batchCount++;
		
		if((_batchCount % ROWS_PER_BATCH) == 0)
		{
			upload2db();				
		}
	}

	private void upload2db() throws IOException
	{
		_batchWriter.close();
		_batchWriter = null;
		
		int rows = DbUtil.loadFromFile(new File(RESULT_LOCATION), BK_TABLE_NAME, 
			Arrays.asList(new String[] { "REPORT_ID", "WTP_ID", "RECENCY", "BK_SEG_ID" }), new UserIdxDb());
		
		if(rows != _batchCount)
			{ _logMail.pf("Warning: uploaded %d rows but batchcount = %d\n", rows, _batchCount);	}
		
		_totalCount += _batchCount;
		_batchCount = 0;
		
		_logMail.pf("Uploaded %d rows, total is %d, user count is %d, %d stage lines read\n", 
			rows, _totalCount, _usersCount, _stageLinesRead);			
	}
	
	private class SortStageList
	{
		private Map<String, Integer> _listHitMap = Util.treemap();
		
		TreeMap<String, Collection<String>> _bufMap = Util.treemap();
		BufferedReader _stageRead;
		
		private boolean _isDoneReading = false;
		
		public SortStageList() throws IOException
		{
			_stageRead = FileUtils.getReader(LOCAL_STAGING_COPY);
			refQ();
		}
			
		private void refQ() throws IOException
		{
			while(_bufMap.size() < TARG_LIST_SIZE && !_isDoneReading)
			{
				String nextline = _stageRead.readLine();
				if(nextline == null)
				{
					_stageRead.close();
					_isDoneReading = true;
					break;
				}
				
				if(nextline.trim().length() == 0)
					{ continue; }
				
				_stageLinesRead++;
				
				String[] wtp_list = nextline.split("\t");
			
				Util.incHitMap(_listHitMap, wtp_list[1]);
				if(_listHitMap.get(wtp_list[1]) > MAX_USERS_PER_LIST)
					{ continue; }
				
				// Require that entries come in order
				Util.massert(_bufMap.isEmpty() || _bufMap.lastKey().compareTo(wtp_list[0]) <= 0,
					"List is out of order: last map key is %s, read %s from file", _bufMap.lastKey(), wtp_list[0]);

				
				Util.setdefault(_bufMap, wtp_list[0], new TreeSet<String>());
				_bufMap.get(wtp_list[0]).add(wtp_list[1]);
			}
		}
		
		public boolean hasNext()
		{
			return !_bufMap.isEmpty();	
		}
		
		public String nextId()
		{
			return _bufMap.firstKey();	
		}
		
		public Map.Entry<String, Collection<String>> nextPack() throws IOException
		{
			Util.massert(hasNext(), "Need to check availability of next pack using hasNext");
			
			Map.Entry<String, Collection<String>> nextpack = _bufMap.pollFirstEntry();
			
			if(_bufMap.size() < TARG_LIST_SIZE / 10)
				{ refQ(); }
			
			return nextpack;
		}
		
	}
}
