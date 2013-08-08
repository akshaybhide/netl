
package com.adnetik.userindex;

import java.sql.*;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;

import com.adnetik.shared.*;

public class AuxDataMonitor
{
	private String _dayCode;
	private SimpleMail _logMail;
	private FileSystem _fSystem;
	
	public static final int SAVE_WINDOW = 90;
	
	// public enum CheckField { bidcount, impcount, clickcount };
	
	public static void main(String[] args) throws IOException
	{		
		AuxDataMonitor adm = new AuxDataMonitor(TimeUtil.getYesterdayCode());
		adm.runCheck();
		adm._logMail.send2admin();
		
		// Two separate mails from same program
		AIMonitorMailReport monrep = new AIMonitorMailReport();
		monrep.runQueryList();
		
	}

	public AuxDataMonitor(String dc) throws IOException
	{
		_logMail = new DayLogMail(this, dc);
		_dayCode = dc;
		
		_fSystem = FileSystem.get(new Configuration());
	}
	
	void runCheck() throws IOException
	{
		deleteOldPcc();
		deleteOldStrip();
		
		List<String> daylist = TimeUtil.getDateRange(SAVE_WINDOW);
		Collections.reverse(daylist);
		
		for(String oneday : daylist)
		{
			String backupdir = Util.sprintf("%s/%s",
						UserIndexUtil.getLocalBackupDir(), oneday);
			
			showLocalDirStats(new File(backupdir));
		}
		
		for(String oneday : daylist)
		{ 
			File stripdir = new File(UserIndexUtil.getLocalPixelStripDir(oneday));
			showLocalDirStats(stripdir);
		}
		
		for(String oneday : daylist)
		{
			File pccdir = (new File(UserIndexUtil.getPccListPath(oneday, "US"))).getParentFile();
			showLocalDirStats(pccdir);
		}
		
		for(String oneday : daylist)
		{
			if(oneday.equals(TimeUtil.getYesterdayCode()))
				{ continue; }
			
			Path negdir = new Path(Util.sprintf("/userindex/negpools/%s", oneday));
			showHdfsDirStats(negdir);
		}		
	}
	
	void deleteOldPcc() throws IOException
	{
		String onepccpath = UserIndexUtil.getPccListPath(_dayCode, "US");
		File pccdir = (new File(onepccpath)).getParentFile().getParentFile();
		deleteOldLocalDir(pccdir, SAVE_WINDOW);		
	}
	
	void deleteOldStrip() throws IOException
	{
		String onedaydir = UserIndexUtil.getLocalPixelStripDir(_dayCode);
		File stripdir = (new File(onedaydir)).getParentFile();
		deleteOldLocalDir(stripdir, SAVE_WINDOW);		
	}
	
	void deleteOldLocalDir(File localdir, int maxsave) throws IOException
	{
		TreeMap<String, File> fileMap = new TreeMap<String, File>(Collections.reverseOrder());
		
		for(File daydir : localdir.listFiles())
		{
			fileMap.put(daydir.getName(), daydir);
		}
			
		while(fileMap.size() > maxsave)
		{
			File delfile = fileMap.pollLastEntry().getValue();
			try 
				{ FileUtils.recursiveDeleteFile(delfile); }
			catch (IOException ioex) 
				{throw new RuntimeException(ioex); }
			_logMail.pf("Deleted directory %s\n", delfile.getAbsolutePath());
		}
	}
	
	void showHdfsDirStats(Path checkme) throws IOException
	{
		if(!_fSystem.exists(checkme))
		{
			_logMail.pf("WARNING directory %s is missing\n", checkme);
			return;			
		}
		
		long totsize = 0;
		long numfile = 1; // start at 1 to avoid div-by-zero errors
		
		for(FileStatus fstat : _fSystem.listStatus(checkme))
		{
			totsize += fstat.getLen();
			numfile++;
		}
		
		_logMail.pf("In dir %s, found %d files, totalsize %d, avg size %d\n", 
			checkme, numfile, totsize, (totsize/numfile));		
	}
	
	void showLocalDirStats(File checkme)
	{
		if(!checkme.exists())
		{
			_logMail.pf("WARNING directory %s is missing", checkme.getAbsolutePath());
			return;
		}
		
		long totsize = 0;
		long numfile = 1; // start at 1 to avoid div-by-zero errors
		
		for(File onefile : checkme.listFiles())
		{
			totsize += onefile.length();
			numfile++;
		}
		
		String pardir = checkme.getParentFile().getName();
		
		_logMail.pf("In dir ../%s/%s, found %d files, totalsize %d, avg size %d\n", 
			pardir, checkme.getName(), numfile, totsize, (totsize/numfile));
	}	
	
	// This is a SEPARATE mailing
	// used to be a python job, now going to do it in Java
	public static class AIMonitorMailReport
	{
		public enum QueryTable { eval_scheme, feature_table, lift_report, party3_report, adaclass_info }
		
		private SortedMap<String, Map<QueryTable, Long>> _countMap =
		 	new TreeMap<String, Map<QueryTable, Long>>(Collections.reverseOrder());
		
		SimpleMail logMail;
		
		String _cutoffDate;
		
		public AIMonitorMailReport()
		{
			logMail = new DayLogMail(this, TimeUtil.getTodayCode());
			
			// Don't want this guy to have timestamps, it screws up the formatting
			logMail.setUseDate(false);
			_cutoffDate = TimeUtil.nDaysBefore(TimeUtil.getTodayCode(), 120);
		}
		
		
		private void runQueryList()
		{
			for(QueryTable qtab : QueryTable.values())
			{
				String sql = getSqlQuery(qtab);
				
				// Util.pf("Going to run for query table %s\n", qtab);	
				// Util.pf("SQL query is %s\n", sql);
				
				List<Pair<java.sql.Date, Number>> reslist = DbUtil.execSqlQueryPair(sql, new UserIdxDb());
				for(Pair<java.sql.Date, Number> onepair : reslist)
				{
					String strdate = onepair._1.toString();
					Util.setdefault(_countMap, strdate, new TreeMap<QueryTable, Long>());
					_countMap.get(strdate).put(qtab, onepair._2.longValue());
				}
			}
			
			// Add a header
			{
				List<Object> header = Util.vector();
				header.add("Date");
				
				for(QueryTable qtab : QueryTable.values())
					{ header.add(qtab); }
				
				printLogRow(header);
			}
			
			for(String onedate : _countMap.keySet())
			{
				List<Object> logrow = Util.vector();
				
				logrow.add(onedate);
				
				for(QueryTable qtab : QueryTable.values())
				{ 
					Long c = _countMap.get(onedate).get(qtab);
					c = (c == null ? 0 : c);
					logrow.add(c);
				}
				
				printLogRow(logrow);
			}
			
			logMail.send2admin();
			
		}
		
		void printLogRow(List<Object> datarow)
		{
			List<String> rowlist = Util.vector();
			
			for(Object oneitem : datarow)
			{ 
				rowlist.add(Util.padstr(oneitem.toString(), 15));
			}
			
			String fullrow = Util.join(rowlist, "");
			logMail.pf("%s\n", fullrow);
		}
		
		private String getSqlQuery(QueryTable qtab)
		{
			return Util.sprintf("SELECT RI.can_day, count(*) from userindex.%s BASET join userindex.report_info RI on " + 
				"BASET.report_id = RI.report_id WHERE RI.can_day > '%s' GROUP BY RI.can_day", qtab, _cutoffDate);
		}
	}
}
