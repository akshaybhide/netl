
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class DataPullDaemon extends Configured implements Tool
{	
	Set<String> compSet = Util.treeset();
	
	// check all files are present
	// check all lengths are correct
	// check directory can be accessed
	
	FileSystem fSystem;
	
	public static void main(String[] args) throws Exception
	{		
		int ecode = ToolRunner.run(new DataPullDaemon(), args);
		//pingNfs();
	}
	
	public int run(String[] args) throws Exception
	{
		fSystem = FileSystem.get(getConf());
		
		while(true)
		{			
			int ccount = 0;
			int tfcount = 0;
			
			for(ExcName exc : ExcName.values())
			{
				for(LogType ltype : new LogType[] { LogType.no_bid_all, LogType.bid_all })
				{
					LogDir ldir = new LogDir(exc, ltype, TimeUtil.getYesterdayCode());
					ccount += testOneDir(ldir);
					tfcount += ldir.hdfsSizeMap.size();
				}
			}
			
			// Prioritize FINISHING yesterday copy, before starting on today's copyu
			if(ccount > 0)
				{ continue; }
			
			String yestcode = TimeUtil.getYesterdayCode();
			
			if(!compSet.contains(yestcode))
			{
				// We've yesterday's data pull, send an AdminMail to indicate success and TIME of completion
				SimpleMail logmail = new SimpleMail("DataPullDaemon Completion Report");
				logmail.addLogLine(Util.sprintf("Completed DataPull for %s", yestcode));
				logmail.addLogLine(Util.sprintf("Copied %d files total", tfcount));
				logmail.send2admin();
				compSet.add(yestcode);	
			}
			
			Util.pf("\nAll files present for yesterday %s", yestcode);
			
			for(ExcName exc : ExcName.values())
			{
				for(LogType ltype : new LogType[] { LogType.no_bid_all, LogType.bid_all })
				{
					LogDir ldir = new LogDir(exc, ltype, TimeUtil.getTodayCode());
					ccount += testOneDir(ldir);
				}
			}		
			
			Util.pf("\nAll files present for today %s", TimeUtil.getTodayCode());
			
			// Sleep for a couple of minutes before rechecking.
			Thread.sleep(1*60*1000);			
		}
	}
	
	int testOneDir(LogDir lDir) throws IOException
	{
		if(!lDir.nfsDirExists())
			{ return 0; }
		
		
		lDir.loadNfsSizeMap();
		lDir.loadHdfsSizeMap();
		return lDir.copyToHdfs();
	}

	public class LogDir
	{
		ExcName exc;
		LogType log;
		String day;
		
		Map<String, Long> nfsSizeMap = Util.treemap();
		Map<String, Long> hdfsSizeMap = Util.treemap();
	
		public LogDir(ExcName e, LogType lt, String d)
		{
			exc = e;
			log = lt;
			day = d;
		}
		
		public boolean nfsDirExists()
		{
			File nfsdir = new File(nfsDirPath());
			return nfsdir.exists();			
		}
		
		public boolean hdfsDirExists() throws IOException
		{			
			Path p = new Path(hdfsDirPath());
			return fSystem.exists(p);
		}
		
		
		void createHdfsDir() throws IOException
		{
			Path p = new Path(hdfsDirPath());
			fSystem.mkdirs(p);
		}
		
		
		void loadNfsSizeMap()
		{
			if(!nfsDirExists())
				{ return; }
			
			File nfsDir = new File(nfsDirPath());
			
			for(File subfile : nfsDir.listFiles())
			{
				nfsSizeMap.put(subfile.getName(), subfile.length());
				
				//Util.pf("\nFile \n\t%s\n\t%d", subfile.getAbsolutePath(), subfile.length());
			}
		}		
		
		void loadHdfsSizeMap() throws IOException
		{
			String logpatt = hdfsDirPath() + "*.log.gz";
			List<Path> pathlist = HadoopUtil.getGlobPathList(fSystem, logpatt);
			
			
			for(Path p : pathlist)
			{
				Long len = fSystem.getFileStatus(p).getLen();
				String[] toks = p.toString().split("/");
				hdfsSizeMap.put(toks[toks.length-1], len);
			}
			
			//Util.pf("\nLoaded %d HDFS files", hdfsSizeMap.size());
		}
		
		int copyToHdfs() throws IOException
		{
			if(!hdfsDirExists())
				{ createHdfsDir(); }
		
			int copycount = 0;
			
			for(String simpname : nfsSizeMap.keySet())
			{
				boolean docopy = false;
				
				if(!hdfsSizeMap.containsKey(simpname))
				{
					Util.pf("\nFile %s not present on hdfs, copying", simpname);
					docopy = true;
					
				} else if(hdfsSizeMap.get(simpname) < nfsSizeMap.get(simpname)) {
				
					Util.pf("\nFile %s has lower size on HDFS, recopying", simpname);
					docopy = true;
					
				}
				
				if(docopy)
				{
					Path src = new Path(nfsDirPath() + simpname);
					Path dst = new Path(hdfsDirPath() + simpname);
					fSystem.copyFromLocalFile(src, dst);
					copycount++;

				} else {
					
					//Util.pf("\nFound file %s on hdfs of matching size", simpname);
				}
			}
			
			//Util.pf("\nCopied %d files for %s", copycount, code());
			return copycount;
		}
		
		
		public String code()
		{
			return Util.sprintf("%s____%s____%s", exc, log, day);
		}
		
		public String nfsDirPath()
		{
			return Util.getNfsDirPath(exc, log, day);
		}
		
		public String hdfsDirPath()
		{
			return Util.sprintf("/tmp/datapulldaemon/%s/%s/%s/", log, day, exc);
		}
		
		
	}
	
}
