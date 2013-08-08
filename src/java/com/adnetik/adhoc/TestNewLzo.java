
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class TestNewLzo
{		
	String _dayCode;
	
	SimpleMail _logMail;
	
	FileSystem _fSystem;
	
	List<ExcName> _excList = Util.vector();
	List<LogType> _typList = Util.vector();
	
	private static LogType[] ICC_LOG_TYPE = new LogType[] { LogType.imp, LogType.click, LogType.conversion };
	
	public static void main(String[] args) throws IOException
	{
		// TODO: replace this with a check that determines if the start day has already passed
		String daycode = "yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0];
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid day code %s", daycode);

		ArgMap amap = Util.getClArgMap(args);
		
		String excliststr = amap.getString("exclist", Util.join(ExcName.values(), ","));
		String typliststr = amap.getString("typlist", Util.join(ICC_LOG_TYPE, ","));
		
		// Util.pf("ExcList: %s\n", Util.join(ExcName.values(), ","));
		
		TestNewLzo newlzo = new TestNewLzo(daycode);
		newlzo.setExcNTypeList(excliststr, typliststr);
		newlzo.runProcess();
	}
	
	TestNewLzo(String dc) throws IOException
	{
		TimeUtil.assertValidDayCode(dc);
		
		_dayCode = dc;
		_logMail = new SimpleMail("NewLzoSynchReport for " + _dayCode);
		
		_fSystem = FileSystem.get(new Configuration());
	}
	
	void setExcNTypeList(String excliststr, String typeliststr)
	{
		for(String onex : excliststr.split(","))
			{ _excList.add(ExcName.valueOf(onex)); }
		
		for(String onet : typeliststr.split(","))
			{ _typList.add(LogType.valueOf(onet)); }
	}
	
	void runProcess() throws IOException
	{
		for(ExcName oneexc : _excList)
		{	
			for(LogType ltype : _typList)
			{
				OneFileOutput ofo = new OneFileOutput(ltype, oneexc);
				ofo.runProcess();				
			}
		}
		
		_logMail.send2admin();
	}
		
	private class OneFileOutput
	{
		
		LogType _logType;
		ExcName _excName;

		SortedSet<String> _logFileSet = Util.treeset();
		LogVersion _targVers;
		
		private long _numRec = 0;
		private long _lzoFSize = 0;
		private long _badLogRec = 0; // hope we don't actually need a long
		
		private double _procTimeSecs; 
		private double _loadTimeSecs;
		
		private int _exitValue; 
		
		OneFileOutput(LogType ltype, ExcName exc)
		{
			_logType = ltype;
			_excName = exc;
			
			List<String> loglist = Util.getNfsLogPaths(_excName, _logType, _dayCode);
			
			if(loglist != null)
				{ _logFileSet.addAll(loglist); }
		}
		
		void runProcess() throws IOException
		{
			if(_logFileSet.isEmpty())
			{
				_logMail.pf("No log files found for LogType=%s, ExcName=%s, skipping\n",
								_logType, _excName);
				return;
			}
			
			double startup = Util.curtime();
			_logMail.pf("Running synch process for LogType=%s, ExcName=%s, found %d files\n",
				_logType, _excName, _logFileSet.size());
			
			// calculate the log version target
			calcLogVersion();
			
			// Remove prev temp file if it exists
			{
				File f = new File(getOutputLzoPath());
				if(f.exists())
				{
					_logMail.pf("TMP file %s already exists, deleting\n", getOutputLzoPath());
					f.delete(); 
				}
			}
			
			String lzopcall = Util.sprintf("lzop -o %s", getOutputLzoPath());
			
			// run the Unix "ps -ef" command
			// using the Runtime exec method:
			Process p = Runtime.getRuntime().exec(lzopcall);
			BufferedWriter bwrite = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()));
			
			int fcount = 0;
			for(String onefile : _logFileSet)
			{
				PathInfo pinfo = new PathInfo(onefile);
				Util.massert(pinfo.pType == _logType && pinfo.pExc == _excName);
				
				BufferedReader bread = FileUtils.getGzipReader(onefile);
				for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
				{
					BidLogEntry ble;
					try { 
						ble = new BidLogEntry(pinfo.pType, pinfo.pVers, oneline);
						
						if(pinfo.pVers != _targVers)
							{ ble = ble.transformToVersion(_targVers); }
						
						ble.basicScrub();
					
					} catch (BidLogFormatException blex) {						
						_badLogRec++;
						continue;
					}
					
					bwrite.write(ble.getLogLine());
					bwrite.write("\n");
					_numRec++;
					
					if((_numRec % 1000000) == 0)
					{
						_logMail.pf("Completed %d records, %d/%d files\n",
							_numRec, fcount, _logFileSet.size());
					}
				}
				bread.close();
				
				fcount++;
			}
			
			
			bwrite.close();	
			
			// Wait for LZOP process to finish. This is mainly necessary 
			// to get the correct LZO file size.
			try { p.waitFor(); } 
			catch (InterruptedException iex) {}
			_exitValue = p.exitValue();

			_procTimeSecs = (Util.curtime() - startup)/1000;
			_lzoFSize = ((new File(getOutputLzoPath())).length());
			
			uploadNClean();
			
			_logMail.pf("Finished, processing took %.03f secs, uploading took %.03f, %d records, %d bad records, LZO file size %d, exit value %d\n", 
				_procTimeSecs, _loadTimeSecs, _numRec, _badLogRec, _lzoFSize, _exitValue);			
		}
		
		private void calcLogVersion()
		{
			SortedSet<LogVersion> lvset = Util.treeset();
			for(String onefile : _logFileSet)
			{
				PathInfo pinfo = new PathInfo(onefile);
				lvset.add(pinfo.pVers);
			}
			_targVers = lvset.first();
			//_logMail.pf("Found LogVersion set %s, using %s\n", 
			// 	lvset, _targVers);			
		}
		
		String getOutputLzoPath()
		{
			return Util.sprintf("/tmp/logsync_%s/%s_%s_%s.lzo", _logType, _excName, _dayCode, _targVers);
		}
		
		void uploadNClean() throws IOException
		{			
			double startup = Util.curtime();
			String hdfspath = HadoopUtil.getHdfsLzoPath(_excName, _logType, _dayCode, _targVers);
			
			Path src = new Path("file://" + getOutputLzoPath());
			Path dst = new Path(hdfspath);
			
			// True, true means overwrite dst and delete src
			_fSystem.copyFromLocalFile(true, true, src, dst);
			
			_procTimeSecs = (Util.curtime() - startup)/1000;
		}
	}
}
