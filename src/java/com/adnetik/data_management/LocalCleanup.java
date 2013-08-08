
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class LocalCleanup
{		
	SortedMap<String, Integer> _daysAgoMap;
	
	SimpleMail _logMail;
	
	String _dayCode;
	
	List<DirSaveInfo> _dsiList = Util.vector();
	List<FileSaveInfo> _fsiList = Util.vector();
	
	public static void main(String[] args) throws Exception
	{
		LocalCleanup lclean = new LocalCleanup();
		lclean.runOp();
	}
	
	public LocalCleanup()
	{
		_dayCode = TimeUtil.getTodayCode();
		_logMail = new SimpleMail("LocalCleanupReport for " + _dayCode);
		
		_dsiList.add(new DirSaveInfo("USERINDEX_STRIP", "/local/bidder_logs/userindex/strip", 90));
		_dsiList.add(new DirSaveInfo("USERINDEX_PCC", "/local/bidder_logs/userindex/pcc", 90));
		_dsiList.add(new DirSaveInfo("USERINDEX_NEGPOOLS", "/local/bidder_logs/userindex/negpools", 90));
		
		// These are "warnings" because they should be deleted by the UserIndex LocalMode code itself
		_dsiList.add(new DirSaveInfo("USERINDEX_SLICE_WARNING", "/local/bidder_logs/userindex/BIGDATA/slice", 60));
		_dsiList.add(new DirSaveInfo("USERINDEX_SHUFF_WARNING", "/local/bidder_logs/userindex/BIGDATA/shuf", 60));
		
		// _dsiList.add(new DirSaveInfo("USERINDEX_SHUFF_WARNING", "/local/bidder_logs/userindex/BIGDATA/shuf", 60));
	}
	
	void runOp() throws IOException
	{
		int dircount = 0; int filecount = 0;
		
		_daysAgoMap = TimeUtil.getDaysAgoMap(300, _dayCode);

		for(DirSaveInfo dsi : _dsiList)
		{
			Pair<File, Integer> killpair = deleteRecentIfValid(dsi);
			
			if(killpair != null)
				{ dircount++; }
		}

		/*
		for(FileSavePolicy fpol : FileSavePolicy.values())
		// for(FileSavePolicy fpol : new FileSavePolicy[] { FileSavePolicy.EX_MASTER, FileSavePolicy.BK_MASTER })
		{
			List<Path> killed = deleteRecentIfValid(fpol);
			
			if(killed != null)
				{ filecount += killed.size(); }
		}
		*/
		
		_logMail.pf("Deleted %d total directories\n", dircount);
		_logMail.pf("Deleted %d total files\n", filecount);

		// _logMail.send2admin();
	}
	
	Pair<File, Integer> findMostRecent(DirSaveInfo dsi) throws IOException
	{
		for(String daycode : _daysAgoMap.keySet())
		{
			String probepath = Util.sprintf("%s/%s", dsi.basedir, daycode);
			
			File daydir = new File(probepath);
			
			if(daydir.exists())
			{
				Util.massert(daydir.isDirectory(), "Target path %s exists but is not a directory", daydir);
				return Pair.build(daydir, _daysAgoMap.get(daycode));
			}
		}

		return null;		
	}
	
	/*
	String findMostRecent(FileSavePolicy fpol) throws IOException
	{
		for(String daycode : daysAgoMap.keySet())
		{
			// Nonstandard use of sprintf, globpatt has the thing we want
			String probepatt = Util.sprintf(fpol.globpatt, daycode);
			List<Path> pathlist = HadoopUtil.getGlobPathList(fSystem, probepatt);	
			
			// Return daycode if any files exist for daycode
			if(pathlist.size() > 0)
				{ return daycode; }
		}		
		
		return null;
	}
	*/
	
	Pair<File, Integer> deleteRecentIfValid(DirSaveInfo dsi) throws IOException
	{
		Pair<File, Integer> killpair = findMostRecent(dsi);
		
		if(killpair == null)
		{
			_logMail.pf("No kill-ready data found for %s, basedir=%s", dsi.toString(), dsi.basedir);
			return null;
		}
			
		if(killpair._2 > dsi.numsave)
		{
			_logMail.pf("Going to delete directory %s, %d days old\n", killpair._1.getAbsolutePath(), killpair._2);
			FileUtils.recursiveDeleteFile(killpair._1);
			return killpair;
		}
		
		return null;
	}
	
	/*
	List<Path> deleteRecentIfValid(FileSavePolicy fpol) throws IOException
	{
		String killday = findMostRecent(fpol);
		
		if(killday == null)
		{
			_logMail.pf("No kill-ready data found for %s, pattern=%s", fpol.toString(), fpol.globpatt);
			return null;
		}
		
		int daysold = daysAgoMap.get(killday);
		
		if(daysold > fpol.savedays)
		{
			String probepatt = Util.sprintf(fpol.globpatt, killday);
			List<Path> pathlist = HadoopUtil.getGlobPathList(fSystem, probepatt);	
			
			for(Path onepath : pathlist)
			{
				_logMail.pf("Going to delete path %s\n", onepath);
				fSystem.delete(onepath, true);
			}

			return pathlist;
		}
		
		return null;
	}	
	*/
	
	
	public static class DirSaveInfo
	{
		String savecode; 
		String basedir;
		int numsave;
		
		private DirSaveInfo(String sc, String pdir, int ns)
		{
			savecode = sc;
			basedir = pdir;
			numsave = ns;
			
			Util.massert(basedir.startsWith("/") && !basedir.endsWith("/"),
				"Invalid basedir %s, should start with / but not end with /", basedir);
		}
		
		public static DirSaveInfo loadFromLine(String logline)
		{
			String[] toks = logline.split(",");
			
			Util.massert(toks.length == 3, "Invalid logline, found %d tokens, want 3", toks.length);
			
			return new DirSaveInfo(toks[0], toks[1], Integer.valueOf(toks[2]));
		}		
		
	}
	
	public static class FileSaveInfo
	{
		String savecode; 
		String pathdir;
		int numsave;
		
		private FileSaveInfo(String sc, String pdir, int ns)
		{
			savecode = sc;
			pathdir = pdir;
			numsave = ns;
		}
		
		public static FileSaveInfo loadFromLine(String logline)
		{
			String[] toks = logline.split(",");
			
			Util.massert(toks.length == 3, "Invalid logline, found %d tokens, want 3", toks.length);
			
			return new FileSaveInfo(toks[0], toks[1], Integer.valueOf(toks[2]));
		}
		

		
	}
}
