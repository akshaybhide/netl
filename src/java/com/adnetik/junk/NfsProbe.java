
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

public class NfsProbe
{	
	/*
	public static final String SAVE_MAP_PATH = "/mnt/burfoot/data_management/nfs_save_map.ser";
	public static final String EXIST_MAP_PATH = "/mnt/burfoot/data_management/nfs_exist_map.ser";
	
	
	TreeMap<String, TreeMap<String, Long>> bigSaveMap;
	TreeMap<String, Boolean> existMap;
	
	double sttNfsTime;
	double endNfsTime;

	
	// check all files are present
	// check all lengths are correct
	// check directory can be accessed 
	
	public static void main(String[] args) throws Exception
	{		
		NfsProbe nprobe = new NfsProbe();
		nprobe.loadSaveMap();
		
		while(true)
		{
			nprobe.probe();
		}
		
		//pingNfs();
	}
	
	void probe()
	{		
		LogPackage lp = LogPackage.random();
		
		sttNfsTime = System.currentTimeMillis();
		Boolean dirExists = checkDirExists(lp);
		endNfsTime = System.currentTimeMillis();

		Util.pf("Took %.03f to access directory\n", (endNfsTime - sttNfsTime)/1000);
		
		if(!existMap.containsKey(lp.code()))
		{
			existMap.put(lp.code(), dirExists);
			writeSaveMap();
			
		} else {
			
			if(existMap.get(lp.code()) && !dirExists)
			{
				throw new RuntimeException("Could not access directory " + lp.nfsdir());
			}
			
			if(!existMap.get(lp.code()) && dirExists)
			{
				// TODO going to need to update this for NFS directory deletion
				Util.pf("Directory has appeared %s", lp.nfsdir());
				existMap.put(lp.code(), dirExists);
				writeSaveMap();
			}
		}

		if(!existMap.get(lp.code()))
		{ 
		//Util.pf(
			return; 
		}

		sttNfsTime = System.currentTimeMillis();
		TreeMap<String, Long> sizemap = getFileSizes(lp);
		endNfsTime = System.currentTimeMillis();
		
		Util.pf("Calculating sizemap took %.04f seconds for %d files \n", (endNfsTime-sttNfsTime)/1000D, sizemap.size());
		
		if(bigSaveMap.containsKey(lp.code()))
		{
			Util.pf("Found code %s\n", lp.code());
			
			TreeMap<String, Long> savemap = bigSaveMap.get(lp.code());
			
			if(sizemap.size() == 0 && savemap.size() > 0)
			{
				Util.pf("Probe found %d files but savemap has %d files\n", 
					sizemap.size(), savemap.size());
				
				throw new RuntimeException("Probe failed for " + lp.code());
			}
			
			if(!savemap.keySet().equals(sizemap.keySet()))
				{ throw new RuntimeException("File list not identical");}
			
			//savemap.remove(savemap.firstKey());
			
			Util.pf("File lists are identical for %s\n", lp.code());
			
			if(!savemap.equals(sizemap))
				{ throw new RuntimeException("Size maps are not identical");	}
			
			Util.pf("Size lists are identical for %s\n", lp.code());
						
		} else {
			Util.pf("Did not find code %s in map, saving...", lp.code());
			
			bigSaveMap.put(lp.code(), sizemap);
			writeSaveMap();
			
			Util.pf("... done\n");
		}
		
		
	}
	
	void loadSaveMap()
	{
		boolean doWrite = false;
		
		try { 
			bigSaveMap = FileUtils.unserializeEat(SAVE_MAP_PATH);
		} catch (Exception ex) {
			
			Util.pf("\nCould not find save map file, creating...");
			ex.printStackTrace();
			
			bigSaveMap = Util.treemap();
			doWrite = true;
		}
		
		try { 
			existMap = FileUtils.unserializeEat(EXIST_MAP_PATH);
		} catch (Exception ex) {
			
			Util.pf("\nCould not find exist set file, creating...");
			
			existMap = Util.treemap();
			doWrite = true;
		}		
		
		if(doWrite)
			{ writeSaveMap(); }
		
	}
	
	void writeSaveMap()
	{
		FileUtils.serializeEat(bigSaveMap, SAVE_MAP_PATH);
		FileUtils.serializeEat(existMap, EXIST_MAP_PATH);
		
	}
	
	boolean checkDirExists(LogPackage lp)
	{
		String nfsdirpath = Util.getNfsDirPath(lp.exc, lp.log, lp.day);
		File nfsdir = new File(nfsdirpath);
		return nfsdir.exists();
	}
	
	TreeMap<String, Long> getFileSizes(LogPackage lp)
	{
		//Util.pf("\ncurrent time is %d", System.currentTimeMillis());
		TreeMap<String, Long> sizemap = Util.treemap();
		
		File nfsDir = new File(lp.nfsdir());
		
		for(File subfile : nfsDir.listFiles())
		{
			sizemap.put(subfile.getAbsolutePath(), subfile.length());
		}
		
		return sizemap;
	}

	public static class LogPackage
	{
		ExcName exc;
		LogType log;
		String day;
		
		private static final Random lprand = new Random();
		
		private static final ExcName[] bigex = Util.bigExchanges();
		private static final LogType[] logtp = LogType.values();
		private static final String[] daycd = TimeUtil.getDateRange("2011-09-01", "2011-10-30").toArray(new String[1]);
		
		
		public LogPackage()
		{
			
			
		}
		
		public static LogPackage random()
		{
			LogPackage lp = new LogPackage();
			lp.exc = bigex[lprand.nextInt(bigex.length)];
			lp.log = logtp[lprand.nextInt(logtp.length)];
			lp.day = daycd[lprand.nextInt(daycd.length)];
			return lp;
		}
		
		public String code()
		{
			return Util.sprintf("%s____%s____%s", exc, log, day);
		}
		
		public String nfsdir()
		{
			return Util.getNfsDirPath(exc, log, day);
		}
		
		public String hdfsTempDir()
		{
			return Util.getHdfsTempDir(exc, log, day);
			
		}
		
		
	}
	*/
}
