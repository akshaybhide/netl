
package com.adnetik.data_management;

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

public class NewLzoLogSync
{		
	String _dayCode;
	
	SimpleMail _logMail;
	
	FileSystem _fSystem;
	
	boolean _overWrite;
	
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
		// IF this is FALSE, we skip paths that already exist
		boolean overwrite = amap.getBoolean("overwrite", false);
		
		NewLzoLogSync newlzo = new NewLzoLogSync(daycode, overwrite);
		newlzo.setExcNTypeList(excliststr, typliststr);
		newlzo.runProcess();
	}
	
	NewLzoLogSync(String dc, boolean overwrite) throws IOException
	{
		TimeUtil.assertValidDayCode(dc);
		
		_dayCode = dc;
		_logMail = new DayLogMail(this, _dayCode);
		
		_fSystem = FileSystem.get(new Configuration());
		
		_overWrite = overwrite;
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
		List<LzoCompiler> complist = Util.vector();
		
		for(ExcName oneexc : _excList)
		{	
			for(LogType ltype : _typList)
			{
				BidLogCompiler blc = new BidLogCompiler(ltype, oneexc);
				complist.add(blc);
			}
		}
		
		complist.add(new PixelCompiler());
		
		for(LzoCompiler lzocomp : complist)
		{
			if(!lzocomp.haveData())
			{ 
				_logMail.pf("No data files found for %s, skipping\n", 
					lzocomp.getCompDesc());
				continue;
			}
			
			/*
			Path mypath = new Path(lzocomp.getHdfsDestPath());
			Util.pf("Path is %s, exists = %b\n", 
				lzocomp.getHdfsDestPath(), _fSystem.exists(mypath));
			*/
				
			if(!_overWrite && _fSystem.exists(new Path(lzocomp.getHdfsDestPath())))
			{
				_logMail.pf("HDFS path %s already exists, skipping\n", 
					lzocomp.getHdfsDestPath());
				continue;
			}	
			
			lzocomp.runProcess();				
		}
		
		_logMail.send2AdminList(AdminEmail.trev, AdminEmail.burfoot);
	}
		
	private abstract class LzoCompiler
	{
		protected SortedSet<String> _logFileSet = Util.treeset();
		
		protected long _numRec = 0;
		protected long _lzoFSize = 0;
		protected long _badLogRec = 0; // hope we don't actually need a long		
		protected int _fCount = 0;
		
		protected double _procTimeSecs; 
		protected double _loadTimeSecs;		
		
		protected int _exitValue; 
		
		abstract String getLocalTmpLzoPath();
		
		abstract String getHdfsDestPath();
		
		abstract String getCompDesc();
		
		abstract void processFile2Writer(String logfilepath, BufferedWriter bwrite) throws IOException;
				
		public boolean haveData()
		{
			return !_logFileSet.isEmpty();	
		}
		
		void uploadNClean() throws IOException
		{			
			double startup = Util.curtime();
			// String hdfspath = HadoopUtil.getHdfsLzoPath(_excName, _logType, _dayCode, _targVers);
			String hdfspath = getHdfsDestPath();
			
			
			Path src = new Path("file://" + getLocalTmpLzoPath());
			Path dst = new Path(hdfspath);
			
			// True, true means overwrite dst and delete src
			_fSystem.copyFromLocalFile(true, true, src, dst);
			
			_procTimeSecs = (Util.curtime() - startup)/1000;
		}	
		
		void runProcess() throws IOException
		{
			Util.massert(!_logFileSet.isEmpty(), "No data files found, check for this elsewhere");
					
			double startup = Util.curtime();
			_logMail.pf("Running synch process for %s, found %d files\n", getCompDesc(), _logFileSet.size());
		
			
			// Remove prev temp file if it exists
			{
				File f = new File(getLocalTmpLzoPath());
				if(f.exists())
				{
					_logMail.pf("TMP file %s already exists, deleting\n", getLocalTmpLzoPath());
					f.delete(); 
				}
			}
			
			String lzopcall = Util.sprintf("lzop -o %s", getLocalTmpLzoPath());
			
			// run the Unix "ps -ef" command
			// using the Runtime exec method:
			Process p = Runtime.getRuntime().exec(lzopcall);
			BufferedWriter bwrite = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()));
			
			for(String onefile : _logFileSet)
			{
				try { processFile2Writer(onefile, bwrite); }
				catch (IOException ioex) {
					
					_logMail.pf("IOException encountered when processing file %s\n", onefile);
					_logMail.addExceptionData(ioex);
				}
				_fCount++;
			}
			
			bwrite.close();	
			
			// Wait for LZOP process to finish. This is mainly necessary 
			// to get the correct LZO file size.
			try { p.waitFor(); } 
			catch (InterruptedException iex) {}
			_exitValue = p.exitValue();

			_procTimeSecs = (Util.curtime() - startup)/1000;
			_lzoFSize = ((new File(getLocalTmpLzoPath())).length());
			
			uploadNClean();
			
			_logMail.pf("Finished, processing took %.03f secs, uploading took %.03f, %d records, %d bad records, LZO file size %d, exit value %d\n", 
				_procTimeSecs, _loadTimeSecs, _numRec, _badLogRec, _lzoFSize, _exitValue);			
		}		
		
	}
	
	private class PixelCompiler extends LzoCompiler
	{

		PixelCompiler()
		{
			List<String> nfspixpath = Util.getNfsPixelLogPaths(_dayCode);
			
			if(nfspixpath != null)
				{ _logFileSet.addAll(nfspixpath); }
		}
		
		protected void processFile2Writer(String onefile, BufferedWriter bwrite) throws IOException
		{
			BufferedReader bread = FileUtils.getGzipReader(onefile);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				PixelLogEntry ple;

				try { 
					ple = new PixelLogEntry(oneline);
					ple.basicCheck();

				} catch (PixelLogEntry.FormatException fex) {						
					_badLogRec++;
					continue;
				}
				
				bwrite.write(ple.getLogLine());
				bwrite.write("\n");
				_numRec++;
				
				if((_numRec % 1000000) == 0)
				{
					_logMail.pf("Completed %d records, %d/%d files\n",
						_numRec, _fCount, _logFileSet.size());
				}
			}
			bread.close();
			
		}
		
		String getLocalTmpLzoPath()
		{
			return Util.sprintf("/tmp/logsync_pixel/pix_%s.lzo", _dayCode);
		}
		
		String getHdfsDestPath()
		{
			return HadoopUtil.getHdfsLzoPixelPath(_dayCode);
		}
		
		String getCompDesc()
		{
			return "PixelLogCompile";	
		}		
		
	}
	
	private class BidLogCompiler extends LzoCompiler
	{
		
		LogType _logType;
		ExcName _excName;

		LogVersion _targVers;
		
		
		BidLogCompiler(LogType ltype, ExcName exc)
		{
			_logType = ltype;
			_excName = exc;
			
			List<String> loglist = Util.getNfsLogPaths(_excName, _logType, _dayCode);
			
			if(loglist != null)
				{ _logFileSet.addAll(loglist); }
			
			extraSetup();
		}
		
		String getCompDesc()
		{
			return Util.sprintf("BidLogCompile for %s, %s", _excName, _logType);	
		}
		
		void extraSetup()
		{
			if(_logFileSet.isEmpty())
				{ return; }
			
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
		
		protected void processFile2Writer(String onefile, BufferedWriter bwrite) throws IOException
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
					
				} catch (Exception ex) {
					_badLogRec++;
					continue;					
				}
				
				bwrite.write(ble.getLogLine());
				bwrite.write("\n");
				_numRec++;
				
				if((_numRec % 1000000) == 0)
				{
					_logMail.pf("Completed %d records, %d/%d files\n",
						_numRec, _fCount, _logFileSet.size());
				}
			}
			bread.close();			
		}
		
		String getLocalTmpLzoPath()
		{
			return Util.sprintf("/tmp/logsync_%s/%s_%s_%s.lzo", _logType, _excName, _dayCode, _targVers);
		}
		
		String getHdfsDestPath()
		{
			return HadoopUtil.getHdfsLzoPath(_excName, _logType, _dayCode, _targVers);
		}
	}
}
