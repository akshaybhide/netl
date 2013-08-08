
package com.adnetik.data_management;

import java.io.IOException;
import java.io.Console;
import java.util.*;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class HdfsCleanup
{		
	public static interface DirSavePolicy
	{
		public abstract String getRootDir();
		public abstract int getSaveDays();
	}
	
	public static interface FileSavePolicy
	{
		public abstract String getPathFormatString();
		public abstract int getSaveDays();
	}	
	
	private enum DirSaveEnum implements DirSavePolicy
	{
		BM_ETL ("/bm_etl/output", 45),
		THIRDPARTY ("/thirdparty/uniqs/basic", 10);  // These go into a database, don't need to keep them for long

		public final String hdfspath;
		public final int savedays;
		
		DirSaveEnum(String hpath, int sdays)
		{
			hdfspath = hpath;
			savedays = sdays;
		}
		
		public String getRootDir()
		{
			return hdfspath;	
		}
		
		public int getSaveDays()
		{
			return savedays;	
		}
	}
	
	
	public enum FileSaveEnum implements FileSavePolicy
	{
		IMPRESSION ("/data/imp/*%s*", 90), // Not that big, can save for a while
		PIXEL ("/data/pixel/*%s*", 300), // Not that big, can save for a while
		EX_MASTER ("/thirdparty/exelate/snapshot/MASTER_LIST*%s.txt", 21),
		BK_MASTER ("/thirdparty/bluekai/snapshot/MASTER_LIST*%s.txt", 15),
		
		PIX_MASTER ("/thirdparty/pix_ourdata/snapshot/MASTER_LIST*%s.txt.gz", 15),
		IAB_MASTER ("/thirdparty/iab_ourdata/snapshot/MASTER_LIST*%s.txt.gz", 15),
		IPP_MASTER ("/thirdparty/ipp_ourdata/snapshot/MASTER_LIST*%s.txt.gz", 15);
		
		
		// DIGI_MASTER ("/thirdparty/digipixel/snapshot/MASTER_LIST*%s.txt", 14);
		
		public final String globpatt;
		public final int savedays;
		
		FileSaveEnum(String gpatt, int sdays)
		{
			globpatt = gpatt;
			savedays = sdays;
		}
		
		public String getPathFormatString()
		{
			return globpatt;	
		}
		
		public int getSaveDays()
		{
			return savedays;	
		}
	}	
	
	FileSystem _fSystem; 
	SortedMap<String, Integer> _daysAgoMap = Util.treemap();	
		
	SimpleMail _logMail;
	
	int _delFileCount = 0;
	int _delDirrCount = 0;
	
	public static void main(String[] args) throws Exception
	{
		HdfsCleanup hclean = new HdfsCleanup();
		
		for(DirSavePolicy basicdir : DirSaveEnum.values())
		{
			hclean.cleanDirectory(basicdir);
		}
		
		for(FileSavePolicy onefile : FileSaveEnum.values())
		{
			hclean.cleanFileList(onefile);	
		}
		
		hclean._logMail.send2AdminList(AdminEmail.trev, AdminEmail.burfoot);
	}
		
	public HdfsCleanup() throws IOException
	{
		_fSystem = FileSystem.get(new Configuration());
		
		String gimp = TimeUtil.getTodayCode();
		
		for(int i = 0; i < 1000; i++)
		{
			_daysAgoMap.put(gimp, i);
			gimp = TimeUtil.dayBefore(gimp);
		}
		
		_logMail = new SimpleMail("HdfsCleanupReport for " + TimeUtil.getTodayCode());	
	}
	
	public void setLogMail(SimpleMail smail)
	{
		_logMail = smail;	
	}
	
	public SimpleMail getLogMail()
	{
		return _logMail;	
	}
	
	public String cleanDirectory(DirSavePolicy onepol) throws IOException
	{
		String killpath = deleteRecentIfValid(onepol);
		
		if(killpath != null)
			{ _delDirrCount++; }
		
		return killpath;
	}
	
	public List<Path> cleanFileList(FileSavePolicy fpol) throws IOException
	{
		List<Path> killed = deleteRecentIfValid(fpol);
		
		if(killed != null)
			{ _delFileCount += killed.size(); }
		
		return killed;
	}
	
	String findMostRecent(DirSavePolicy spol) throws IOException
	{
		for(String daycode : _daysAgoMap.keySet())
		{
			String probepath = Util.sprintf("%s/%s", spol.getRootDir(), daycode);
			
			if(_fSystem.exists(new Path(probepath)))
			{ 
				// Util.pf("Found probepath %s for daycode %s\n", probepath, daycode);	
				return daycode;
			}
		}
		return null;		
	}
	
	String findMostRecent(FileSavePolicy fpol) throws IOException
	{
		for(String daycode : _daysAgoMap.keySet())
		{
			// Nonstandard use of sprintf, globpatt has the thing we want
			String probepatt = Util.sprintf(fpol.getPathFormatString(), daycode);
			List<Path> pathlist = HadoopUtil.getGlobPathList(_fSystem, probepatt);	
			
			// Return daycode if any files exist for daycode
			if(pathlist.size() > 0)
				{ return daycode; }
		}		
		
		return null;
	}
	
	String deleteRecentIfValid(DirSavePolicy spol) throws IOException
	{
		String killday = findMostRecent(spol);
		
		if(killday == null)
		{
			_logMail.pf("No kill-ready data found for %s, basedir=%s", spol.toString(), spol.getRootDir());
			return null;
		}
		
		int daysold = _daysAgoMap.get(killday);
		
		if(daysold > spol.getSaveDays())
		{
			String delpath = Util.sprintf("%s/%s", spol.getRootDir(), killday);
			int tooold = daysold - spol.getSaveDays();
			_logMail.pf("Going to delete-ready daycode %s, %d days old, excess_age=%d, spol %s\n\tpath=%s\n", 
				killday, daysold, tooold, spol, delpath);
			_fSystem.delete(new Path(delpath), true);
			return delpath;
		}
		
		return null;
	}
	
	List<Path> deleteRecentIfValid(FileSavePolicy fpol) throws IOException
	{
		String killday = findMostRecent(fpol);
		
		if(killday == null)
		{
			_logMail.pf("No kill-ready data found for %s, pattern=%s", fpol.toString(), fpol.getPathFormatString());
			return null;
		}
		
		int daysold = _daysAgoMap.get(killday);
		
		if(daysold > fpol.getSaveDays())
		{
			String probepatt = Util.sprintf(fpol.getPathFormatString(), killday);
			List<Path> pathlist = HadoopUtil.getGlobPathList(_fSystem, probepatt);	
			int tooold = daysold - fpol.getSaveDays();
			
			for(Path onepath : pathlist)
			{
				_logMail.pf("Going to delete path %s, excess_age=%d\n", onepath, tooold);
				_fSystem.delete(onepath, true);
			}

			return pathlist;
		}
		
		return null;
	}	
	
}
