package com.adnetik.fastetl;

import java.util.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.*;

import java.util.List;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import java.io.*;

import com.adnetik.fastetl.FastUtil.*;

public class AggregationEngine 
{
	QuatlyAgg _quatAgg;
	CookieAgg _cookAgg;
	
	FileManager _fileMan;
	InterestManager _intMan; 
	
	public static void main(String[] args) throws Exception
	{
		AggregationEngine aggEng = new AggregationEngine("2012-05-29", 1);
		
		aggEng.flushToStaging();
		
		// aggEng.checkLockMove();
		
	}	
	public AggregationEngine(String date, int lookback) throws IOException{
		_fileMan = new FileManager();
		_quatAgg = new QuatlyAgg();
		_quatAgg.initAggregators(date, lookback);
		_cookAgg = new CookieAgg();
		_intMan = new InterestManager();
		_intMan.loadCurrent();
		loadFromSaveData();
		startUp(date , lookback);
	}
	void loadFromSaveData() throws IOException
	{
		
		
		// load from CURRENT (=true)
		Set<String> savedata = FastUtil.getAggPathSet(true);
		Util.pf("Found %d save data paths\n", savedata.size());
		
		_fileMan.reloadFromSaveDir();
		_quatAgg.loadFromSaveData(savedata);
		_cookAgg.loadFromSaveData(savedata);
		
	}
	
	void startUp(String date, int lookback)
	{
		Set<String> pathset = _fileMan.newFilesLookBack(date, lookback);
		List<String> pathlist = new Vector<String>(pathset);
		Collections.shuffle(pathlist);
		
		Util.pf("Found %d new paths\n", pathset.size());
		
		for(int i = 0;i < pathlist.size() ; i++)
		{
			String onepath  = pathlist.get(i);
			processFile(onepath);
			
			if((i % 10) == 0)
			{
				Util.pf("Done with file %d/%d\n", i, pathlist.size());
			}
			
			if(i >= 1000)
				{ break; }
		}
	}
	
	void flushToStaging() throws IOException
	{
		_quatAgg.writeToStaging();
		_cookAgg.writeToStaging();
		_fileMan.flushCleanList();
	}
	

		
	void processFile(String filepath)
	{
		// TODO: this is kind of ugly, shouldn't need to read the file twice.
		// Util.pf("Running for file %s\n", filepath);
		Util.pf(".");
		
		try { 
			BufferedReader bread = FastUtil.getGzipReader(filepath);
			
			if(filepath.indexOf("pixel") != -1) 
				{ 
					long start = System.currentTimeMillis();
					
					runForPixel(bread);
					long end = System.currentTimeMillis();
					Util.pf("Execution time for runforPixel "+(end-start)+" ms.\n");
					
				}
			else
			{
				PathInfo pinfo = new PathInfo(filepath);
				long start = System.currentTimeMillis();
				runForIcc(bread, pinfo.pType, pinfo.pVers);
				long end = System.currentTimeMillis();
				Util.pf("Execution time for runforICC "+(end-start)+" ms.\n");
			}
			bread.close();
			
		} catch (IOException ioex) { 
			throw new RuntimeException(ioex);	
		}		
		
		_fileMan.reportFinished(filepath);
	}
	
	private void runForIcc(BufferedReader bread, LogType logtype, LogVersion relvers) throws IOException
	{
		MyLogType mlt = MyLogType.valueOf(logtype.toString());
		
		int usefulcooki = 0;
		int usefullineitem = 0;
		int lines = 0;
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			lines++;
			BidLogEntry ble = BidLogEntry.getOrNull(logtype, relvers, oneline);
			if(ble == null)
				{ continue; } // TODO: do something here?

			if(_cookAgg.processLogEntry(mlt, ble, _intMan.getLineItemInterest()))
				usefulcooki++;
			if(_quatAgg.processLogEntry(mlt, ble, _intMan.getLineItemInterest()))
				usefullineitem++;
		}
		
		//Util.pf("useful lines for cookie :%d\n" , usefulcooki);
		//Util.pf("useful lines for lineitem :%d\n" , usefullineitem);
		Util.pf("number of lines of the file :%d\n" , lines);
		
	}
	
	private void runForPixel(BufferedReader bread) throws IOException
	{
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			PixelLogEntry ple = PixelLogEntry.getOrNull(oneline);
			if(ple == null)
				{ continue; } // TODO: do something here?	
			
			_cookAgg.processLogEntry(MyLogType.pixel, ple, _intMan.getPixelInterest());
			_quatAgg.processLogEntry(MyLogType.pixel, ple, _intMan.getPixelInterest());
		}
	}		

	void checkLockMove() throws Exception
	{
		lock();
		{
			File jnk = new File(FastUtil.getJunkPath());
			File cur = new File(FastUtil.getBaseDir(true));
			File stg = new File(FastUtil.getBaseDir(false));
			
			// TODO: delete junk directories
			
			// Should be very fast operation
			cur.renameTo(jnk);
			stg.renameTo(cur);
			Util.pf("Renamed directory %s --> %s\n", stg, cur);			
		}
		unlock();
	}	
	
	void lock() throws Exception
	{
		String dir = FastUtil.BASE_PATH;
		File otherslock = new File(dir + "/jason.lock");
		if(!otherslock.exists()){
			File ourlock = new File(dir + "/fastetl.lock");
			ourlock.createNewFile();
			return;
		}
		while(otherslock.exists())
			Thread.sleep(5000);
		// if lock is there wait
		//else create a lock and return
	}
	
	void unlock() throws Exception{
		//remove the lock file\
		String dir = FastUtil.BASE_PATH;
		File ourlock = new File(dir +"/fastetl.lock");
		if(ourlock.exists()){
			ourlock.delete();
			return;
		}
		throw new Exception("there was supposed to be a lock here!");
	}		
}

