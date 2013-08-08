
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import java.nio.channels.FileChannel;


import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.data_management.NfsProbe.*;


public class ProbeCopyNfs 
{
	// Allow 2 minutes
	public static final long MAX_ALLOW_TIME_MILLIS = 2*60*1000;
	
	int copyCount = 0;
	int sameCount = 0;
	int totlCount = 0;
	int failCount = 0;
	
	int packCheckCount = 0;
	int failReadCount = 0;
	
	FileSystem fSystem;
	
	/*
	
	public static void main(String[] args) throws Exception
	{
		int ecode = ToolRunner.run(new ProbeCopyNfs(), args);
		System.exit(ecode);
		
		
		ProbeCopyNfs pcn = new ProbeCopyNfs();
		
	}
	
	public int run(String[] args) throws IOException
	{
		fSystem = FileSystem.get(getConf());
		
		if(fSystem == null)
		{ 
			Util.pf("\nrror, fsystem is null");
			return -1;
		}
		
		String yestcode = TimeUtil.getYesterdayCode();
		String tdaycode = TimeUtil.getTodayCode();
		
		timeLimitedProbe(yestcode);
		timeLimitedProbe(tdaycode);
		
		return 1;

	}
	
	void timeLimitedProbe(String daycode) throws IOException
	{
		Util.pf("\nSTART>>> calling ProbeCopyNfs for %s", daycode);
		
		long start = System.currentTimeMillis();
		
		for(LogPackage lpack : iccLogs(daycode))
		{
			boolean succ = probeCopy(lpack);
			
			packCheckCount += (succ ? 1 : 0);
			
			Util.pf("\nFinished probeCopy for %s", lpack.code());
			
			// Going to run this in a cron job, so stop if it goes over certain allowed time limit
			// otherwise we're going to get multiple versions of this thing running simultaneously
			if(System.currentTimeMillis() - start > MAX_ALLOW_TIME_MILLIS)
			{ 
				Util.pf("\nHit time limit, terminating");
				break; 
			} 
		}
		
		Util.pf("\nChecked %d directories, failed to read %d times",
			packCheckCount, failReadCount);

		Util.pf("\n\tFound %d files total, %d copied, %d already present", 
			totlCount, copyCount, sameCount);
	}
	
	List<LogPackage> iccLogs(String daycode)
	{
		List<LogPackage> lplist = Util.vector();
		
		for(ExcName exc : ExcName.values())
		{
			for(LogType log : new LogType[] { LogType.conversion, LogType.click, LogType.imp })
			{
				LogPackage lp = new LogPackage();
				lp.exc = exc;
				lp.log = log;
				lp.day = daycode;
				lplist.add(lp);
			}	
		}
		
		Collections.shuffle(lplist);
		return lplist;	
	}
	
	boolean probeCopy(LogPackage lpack) throws IOException
	{		
		Map<String, Long> dstmap = getHdfsSizeMap(lpack.hdfsTempDir());
		Map<String, Long> srcmap;
	
		try {
			srcmap = nfsSizeMap(lpack);
		} catch (Exception ex) {
			
			Util.pf("\nError reading directory %s", lpack.nfsdir());
			failReadCount++;
			return false;
		}
				 
				
		for(String srcfile : srcmap.keySet())
		{
			totlCount++;
			
			if(!dstmap.containsKey(srcfile) || srcmap.get(srcfile) > dstmap.get(srcfile))
			{
				try { 
					copyToHdfs(lpack, srcfile); 
					copyCount++;
				} catch (IOException ioex) {
					
					failCount++;
				}
					
			} else {
				// Sanity check: local file can't ever be LARGER than NFS file, right?
				Util.massertEq(dstmap.get(srcfile), srcmap.get(srcfile));
				sameCount++;
			}
		}
		
		return true;
	}
	

	
	Map<String, Long> locSizeMap(LogPackage lpack)
	{
		return null;
		//return getFileSizeMap(lpack.localTmpDir());
	}
	
	Map<String, Long> nfsSizeMap(LogPackage lpack)
	{
		return getFileSizeMap(lpack.nfsdir());
	}
	
	public static Map<String, Long> getFileSizeMap(String path)
	{
		//Util.pf("\ncurrent time is %d", System.currentTimeMillis());
		TreeMap<String, Long> sizemap = Util.treemap();
		
		File direct = new File(path);
		
		for(File subfile : direct.listFiles())
		{
			sizemap.put(subfile.getName(), subfile.length());
		}
		
		return sizemap;		
	}	
	
	Map<String, Long> getHdfsSizeMap(String path) throws IOException
	{
		//Util.pf("\ncurrent time is %d", System.currentTimeMillis());
		Path hdfspath = new Path(path);
		TreeMap<String, Long> sizemap = Util.treemap();
		
		if(!fSystem.exists(hdfspath))
		{
			fSystem.mkdirs(hdfspath);
			Util.pf("\nCreated hdfs directory %s", hdfspath);
		}
		
		for(FileStatus fstat : fSystem.listStatus(hdfspath))
		{
			//Path subfile = fstat.getPath();
			String fname = fstat.getPath().getName();
			sizemap.put(fname, fstat.getLen());
		}
		
		return sizemap;		
	}		
	
	
	public void copyToHdfs(LogPackage lpack, String fname) throws IOException
	{
		// Add local file prefix.
		Path src = new Path("file://" + lpack.nfsdir() + "/" + fname);
		Path dst = new Path(lpack.hdfsTempDir() + "/" + fname);
		
		fSystem.copyFromLocalFile(false, false, src, dst);
		
		//Util.pf("\nFile copy successful \n\tsrc=%s\n\tdst=%s", src, dst);
		
	}
	
	/*
	public static void copyFile(File sourceFile, File destFile) throws IOException {
		if(!destFile.exists()) {
			destFile.createNewFile();
		}
		
		FileChannel source = null;
		FileChannel destination = null;
		
		try {
			source = new FileInputStream(sourceFile).getChannel();
			destination = new FileOutputStream(destFile).getChannel();
			destination.transferFrom(source, 0, source.size());
			//Util.pf("\nFile transfer successful: %s, %s", sourceFile, destFile);
		} 
		finally {
			if(source != null) {
				source.close();
			}
			if(destination != null) {
				destination.close();
			}
		}
	}
	
	boolean copyToTemp(LogPackage lpack, String fname)
	{
		File src = new File(lpack.nfsdir() + "/" + fname);
		File dst = new File(lpack.localTmpDir() + "/" + fname);
		
		try { 
			copyFile(src, dst); 
			Util.sprintf("\nFile copy successful: \n\tsrc=%s\n\tdst=%s", src, dst);
			return true;
			
		} catch (IOException ioex) {
			
			Util.sprintf("\nError on file  copy : \n\tsrc=%s\n\tdst=%s", src, dst);
			return false;
		}
		throw new RuntimeException("nyi");
	}
	*/
	
}
