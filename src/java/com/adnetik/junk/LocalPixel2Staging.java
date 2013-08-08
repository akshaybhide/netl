
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

import com.adnetik.userindex.UserIndexUtil.*;


public class LocalPixel2Staging
{		
	public static void main(String[] args) throws IOException
	{
		if(args.length < 1)
		{ 
			Util.pf("Usage: LocalPixel2Staging <daycode>");
			return;
		}
		
		Map<String, String> clargs = Util.getClArgMap(args);
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		boolean dostrip = !("false".equals(clargs.get("dostrip")));
		boolean docompile = !("false".equals(clargs.get("docompile")));
		
		if(dostrip)
		{
			List<String> pathlist = Util.getNfsPixelLogPaths(daycode);
			Collections.shuffle(pathlist);
			
			DayPixBag dpb = new DayPixBag(daycode);
			dpb.cleanLocalDir();
			
			double startup = Util.curtime();
			for(int i = 0; i < pathlist.size(); i++)
			{
				String onepath = pathlist.get(i);
				dpb.processFile(onepath);
				
				if((i % 10) == 0)
					{ Util.pf("Finished with file %d/%d, average is %.03f\n", i, pathlist.size(), (Util.curtime()-startup)/(1000*(i+1)));}
			}
			
			// Final flush
			dpb.flush();
		}
		
		if(docompile)
		{
			compile2Hdfs(daycode);
		}
	}
	
	static void compile2Hdfs(String daycode) throws IOException
	{
		if(!UserIndexUtil.isBlockStartDay(daycode))
			{ return; }
		
		Set<String> lcodeset = ListInfoManager.getSing().getListenCodesByPref("pixel");		
		FileSystem fsys = FileSystem.get(new Configuration());
		String stagingpath = UserIndexUtil.getStagingInfoPath(StagingType.pixel, daycode);
		
		Writer bwrite = HadoopUtil.getHdfsWriter(fsys, stagingpath);
		for(String onelist : lcodeset)
		{
			int onecode = UserIndexUtil.pixelIdFromCode(onelist);
			compileNWrite(daycode, onecode, bwrite);
		}
		bwrite.close();
	}
	
	static void compileNWrite(String daycode, int pixid, Writer bwrite) throws IOException
	{
		Util.pf("Reading for pixel %d, daycode=%s\n", pixid, daycode);
		List<String> daylist = TimeUtil.getDateRange(daycode, 30);
		List<String> idlist_pathlist = Util.vector();
		
		for(String oneday : daylist)
		{
			String strippath = UserIndexUtil.getLocalPixelStripPath(oneday, pixid);
			idlist_pathlist.add(strippath);
		}
		
		String listcode = Util.sprintf("pixel_%d", pixid);
		compileNWrite(idlist_pathlist, listcode, bwrite);
	}
	
	// Reads WTP ids from a list of paths, writes to the given writer form <ID, listcode> pairs
	static void compileNWrite(List<String> idlistpaths, String listcode, Writer bwrite) throws IOException
	{
		Util.pf("Reading from %d id list paths\n", idlistpaths.size());
		SortedSet<WtpId> idset = Util.treeset();
		
		for(String onepath : idlistpaths)
		{
			if(!(new File(onepath)).exists())
				{ continue; }
			
			List<String> idlines = FileUtils.readFileLinesE(onepath);		
			Util.pf(".");
			for(String oneid : idlines)
			{
				WtpId wid = WtpId.getOrNull(oneid.trim());
				if(wid != null)
					{ idset.add(wid); }
			}
		}
		
		Util.pf(" done reading, now writing\n");
		
		for(WtpId wid : idset)
		{
			String towrite = Util.sprintf("%s\t%s\n", wid.toString(), listcode);	
			bwrite.write(towrite);
		}
	
		Util.pf("Found %d total ids for listcode %s\n", idset.size(), listcode);			
	}

	
	public static class DayPixBag
	{
		public static final int COMFORT_SIZE = 100000;
		
		SortedSet<Pair<Integer, WtpId>> batchData = Util.treeset();
		
		String dayCode;
		
		public DayPixBag(String dc)
		{
			dayCode = dc;	
			
			FileUtils.createDirForPath(UserIndexUtil.getLocalPixelStripPath(dayCode, 0));
		}
		
		void cleanLocalDir()
		{
			File localdir = new File(UserIndexUtil.getLocalPixelStripDir(dayCode));
			int delcount = 0;
			
			for(File stripfile : localdir.listFiles())
			{
				// Util.pf("Going to delete strip file %s\n", stripfile.getName());			
				stripfile.delete();
				delcount++;
			}
			
			Util.pf("Deleted %d previous local files\n", delcount);
		}
			
		public void processFile(String pixlogpath) throws IOException
		{	
			// Util.pf("Processing file %s\n", pixlogpath);
			
			BufferedReader bread = Util.getGzipReader(pixlogpath);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				PixelLogEntry ple = new PixelLogEntry(oneline);
				
				int pixid = ple.getIntField("pixel_id");
				String wtpid = ple.getField("wtp_user_id");
								
				WtpId wid = WtpId.getOrNull(wtpid);
				if(wid != null)
					{ batchData.add(Pair.build(pixid, wid)); }
			}
			bread.close();
			
			flushIfNecessary();
		}
		
		void flushIfNecessary() throws IOException
		{
			if(batchData.size() < COMFORT_SIZE)
				{ return; }

			flush();
		}
		
		void flush() throws IOException
		{
			Util.pf("Flushing PixData, size is %d\n", batchData.size());
			
			int curpix = -1;
			BufferedWriter appwrite = null;
			
			for(Pair<Integer, WtpId> onepair : batchData)
			{
				int newpix = onepair._1;
				
				if(newpix != curpix)
				{
					if(appwrite != null)
						{ appwrite.close(); }	
					
					// Open a new writer in append mode Append=true
					File pixstrip = new File(UserIndexUtil.getLocalPixelStripPath(dayCode, newpix));
					appwrite = new BufferedWriter(new FileWriter(pixstrip, true));
					
					curpix = newpix;
				}
				
				appwrite.write(onepair._2 + "\n");
			}
					
			if(appwrite != null)
				{ appwrite.close(); }		
			
			batchData.clear();			
		}
	}
}
