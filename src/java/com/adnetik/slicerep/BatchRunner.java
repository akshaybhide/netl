package com.adnetik.slicerep;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

/**
 * Main control loop for the Local Batch aggregation jobs.
 * Uses a ControlTable to keep track of files.
 * Also logs a bunch of statistics for each batch.
 */ 
public class BatchRunner 
{	
	private String 		_dayCode;
	private QuarterCode 	_qrtCode;
	
	private ControlTable _cTable;
	
	// private Set<String> _slowGlobalSet = Util.treeset();
	
	private int _sleepCount = 0; // Number of times we've slept since last run
	
	public static long SLEEP_SEC = 5;
	
	// Number of counts since the last stating->main pull, count since mail
	// Going to start this at 4, so we don't have to wait for the pull.
	private int _countSincePull = 4;
	// private int _countSincePull = 0;
	private int _countSinceMail = 0;

	private SimpleMail _pullMail;
	
	private SortedMap<String, ClickChecker> _checkMap = Util.treemap();

	public static void main(String[] args) throws Exception
	{
		Util.pf("Running BatchRunner...\n");
		
		
		try {
			BatchRunner brun = new BatchRunner();
			brun.simpleMain();
		} catch (Exception ex) {
			
			ex.printStackTrace();
			SimpleMail.sendExceptionMail("SliceRep Local ETL", ex);
			throw ex;
		}
	}
	
	BatchRunner()
	{		
		_cTable = new ControlTable.CleanListImpl(SliUtil.CLEAN_LIST_DIR);
		
		setNextRunTime();
		
		Util.pf("Daycode is %s, qcode is %s\n", _dayCode, _qrtCode.toTimeStamp());
		// System.exit(1);
		// _cTable = new ControlTable(_dayCode, SliUtil.LOOKBACK);
	}
	
	void simpleMain() throws Exception
	{
		int fallbehind = 1; // initialize to 1 so we don't wait for the first run
		CatalogUtil.initSing(null, DbTarget.internal);		

		Util.pf("Look back is %d days\n", SliUtil.LOOKBACK);
		
		
		// PathInfoManager pim = new PathInfoManager.DumbDanImpl();
		LogType[] touse = new LogType[] { LogType.bid_all, LogType.imp, LogType.click, LogType.conversion };
			
		//LogType[] touse = new LogType[] {  LogType.click, LogType.conversion };	
		
		while(true)
		{
			Util.pf("Next block to run for is %s %s, sleeping... \n", _dayCode, _qrtCode.toTimeStamp());

			// Only sleep/waste time if we're completely up-to-date, fallbehind=0
			while(fallbehind == 0 && !timeToStart(false))
			{ 
				Util.pf("z"); // zzzzzzzzzz.... 
				Thread.sleep(SLEEP_SEC*1000); 

				// 15 minutes = 900 seconds, so 3000 is way too long				
				if(_sleepCount++ * SLEEP_SEC > 3000)
				{
					Util.pf("Error: slept for too long!!!");
					timeToStart(true); // Print out info about start/current/etc
					throw new RuntimeException("Sleeping sickness");
				}
			}	
			
			_sleepCount = 0;
			Util.pf("\nTime to start for %s %s\n", _dayCode, _qrtCode.toTimeStamp());
			
			CatalogUtil.getSing().refreshCreativeLookup();			
			
			SortedSet<String> pathset = _cTable.nextBatch(SliUtil.LOOKBACK, SliUtil.MAX_BATCH);
			fallbehind = _cTable.getFallBehind();
			
			if(pathset.size() > 0)
			{
				// Breakmap = Map<daycode, pathset for that daycode>
				SortedMap<String, SortedSet<String>> breakmap = sortByNfsDate(pathset);
				String relday = breakmap.firstKey();
				SortedSet<String> onedayset = breakmap.get(relday);				
				
				Util.pf("Running for %d paths, fallbehind is %d, \nfirst=%s\nlast=%s\n", 
					pathset.size(), fallbehind, onedayset.first(), onedayset.last());
				
				ClickChecker relchecker = getRelChecker(relday);
				LocalBatch lbatch = new LocalBatch(relday, onedayset, relchecker);
				lbatch.doIt();	
				
				// Write some extra log data
				{
					lbatch._bStats.setField("fallbehind", _cTable.getFallBehind());
					lbatch._bStats.setField("frstfile", onedayset.first());
					lbatch._bStats.setField("lastfile", onedayset.last());
					lbatch.writeLogData(SliUtil.getNewStatLogPath(_dayCode, _qrtCode));	
				
				}
				
				// Save the clean list and click-check data
				// Key point: we need to do the write of the click-check data at the same moment
				// as the write of the clean list.
				{
					_cTable.reportFinished(onedayset);
					relchecker.flushData();
				}
				
				// checkRunSlowGlobal();
				
				{
					// Okay i am DECOUPLING the main ETL from the Stage2Main stuff
					Calendar lastcal = TimeUtil.calFromNfsPath(onedayset.last());
					checkRunPull(lastcal);
				}
				
			} else {
				Util.pf("Found no new paths for this block\n");
			}
			
			setNextRunTime();
		}
	}
	
	private void sendPullMailMaybe()
	{
		if(_countSinceMail == 4)
		{
			_pullMail.send2admin();	
			_pullMail = null;
			_countSinceMail = 0;
		}
		
		_countSinceMail++;
	}
	
	private void initPullMailMaybe()
	{
		if(_pullMail == null)
		{
			_pullMail = new SimpleMail("Stage2Main Pull Report for " + TimeUtil.getTodayCode());
		}
	}
	
	private void checkRunPull(Calendar lastcal)
	{
		Util.pf("CountSincePull=%d\n", _countSincePull);
		
		initPullMailMaybe();
		
		if(_countSincePull == 4)
		{
			for(AggType atype : AggType.values())
			{
				//List<Stage2MainPull.WhereBatch> wblist = Stage2MainPull.getCampTimeBatchList(lastcal);
				// Stage2MainPull s2mp = new Stage2MainPull(atype, _pullMail, wblist);
				// for(DbTarget onetarg : new DbTarget[] { DbTarget.new_internal, DbTarget.internal })
				for(DbTarget onetarg : Util.listify(DbTarget.internal))
				{
					Stage2MainPull.WhereBatch onewhere = Stage2MainPull.getDefaultWhereBatch(lastcal);				
					Stage2MainPull s2mp = new Stage2MainPull(atype, _pullMail, onewhere);
					s2mp.runAllUpdates(onetarg);					
				}
			}
					
			_countSincePull = 0;
		}		

		_countSincePull++;
		
		sendPullMailMaybe();
	}
	
	boolean timeToStart(boolean doprint)
	{
		// Going to do all the time-start logic using string comparisons
		String crrnt_time = Util.cal2LongDayCode(new GregorianCalendar());
		String start_time = startTimeForBlock(_dayCode, _qrtCode);
		int comparison = crrnt_time.compareTo(start_time);
		
		if(doprint)
		{
			Util.pf("Block time is %s %s, \nstart time is %s, \ncurrent time  is %s\n", 
				_dayCode, _qrtCode.toTimeStamp(), start_time, crrnt_time);
			Util.pf("Comparison is %d\n", comparison);
		}
		
		return comparison > 0;
	}
	
	static String startTimeForBlock(String daycode, QuarterCode qcode)
	{
		String tstamp = Util.sprintf("%s %s", daycode, qcode.toTimeStamp());
		Calendar mycal = Util.longDayCode2Cal(tstamp);
		
		// Okay, the files should show up on the 15:00 block, but 
		// we want to add a bit of an offset 
		mycal.add(Calendar.MINUTE, 7);
		return Util.cal2LongDayCode(mycal);
	}

	// 
	void setNextRunTime()
	{
		if(_dayCode != null && _qrtCode != null)
		{ 
			Util.pf("Incrementing block, cur BLOCK is %s %s, STAMP is %s \n", 
				_dayCode, _qrtCode.toTimeStamp(), Util.cal2LongDayCode(new GregorianCalendar()));
		}
		
		String[] date_time = Util.cal2LongDayCode(new GregorianCalendar()).split(" ");
		_dayCode = date_time[0];
		_qrtCode = QuarterCode.prevQuarterFromTime(date_time[1]);
		
		Util.pf("Before increment, qcode is %s, after is %s\n", 
			_qrtCode.toTimeStamp(), (_qrtCode.nextQuarter() == null ? "EndOfDay" : _qrtCode.nextQuarter().toTimeStamp()));
		
		_qrtCode = _qrtCode.nextQuarter();
		if(_qrtCode == null)
		{ 
			// Increment
			_dayCode = TimeUtil.dayAfter(_dayCode); 
			_qrtCode = QuarterCode.getFirstQuarter();
		}
		
		Util.pf(" ... new BLOCK is %s %s \n", _dayCode, _qrtCode.toTimeStamp());
		
	}
	
	public static SortedMap<String, SortedSet<String>> sortByNfsDate(Collection<String> pathbag)
	{
		SortedMap<String, SortedSet<String>> datemap = Util.treemap();
		for(String onepath : pathbag)
		{
			Calendar onecal = TimeUtil.calFromNfsPath(onepath);
			String daycode = TimeUtil.cal2DayCode(onecal);
			
			Util.setdefault(datemap, daycode, new TreeSet<String>());
			datemap.get(daycode).add(onepath);
		}
		return datemap; 
	}	
	
	
	private ClickChecker getRelChecker(String relday)
	{
		Util.massert(TimeUtil.checkDayCode(relday), "Invalid day code %s", relday);
		
		// If it's in the map, it should be good
		if(!_checkMap.containsKey(relday))
		{
			File logsavedir = new File(Util.sprintf("%s/%s", SliUtil.CLICK_SAVE_DIR, relday));
			if(!logsavedir.exists())
			{ 
				Util.pf("Creating click log save dir %s\n", logsavedir.getAbsolutePath());
				logsavedir.mkdir(); 
			}
			
			// If the directory exists and batch files are already present, 
			// this will rebuild the ClickChecker using those files.
			ClickChecker clickcheck = new ClickChecker(logsavedir);
			_checkMap.put(relday, clickcheck);
		}			
		
		return _checkMap.get(relday);
	}
}
