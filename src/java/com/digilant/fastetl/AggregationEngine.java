package com.digilant.fastetl;

import java.util.*;

import com.adnetik.shared.BidLogEntry;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.PathInfo;
import com.adnetik.shared.PixelLogEntry;
import com.adnetik.shared.Util;
import com.adnetik.shared.Util.LogType;
import com.adnetik.shared.Util.LogVersion;

import java.util.List;

import java.io.*;

import com.digilant.fastetl.FastUtil.*;

public class AggregationEngine 
{
	QuatlyAgg _quatAgg;
	CookieAgg _cookAgg;
	
	FileManager _fileMan;
	InterestManager _intMan; 
	List<String> brokenfiles = new Vector<String>();
	
	public static void main(String[] args) throws Exception
	{
		if(args.length < 3){
			Util.pf( "you need to pass date (as 2012-05-29) and the number of look back date \n");
			System.exit(1);
		}
		String date = args[0];
		int lookback = Integer.parseInt(args[1]);
		String path = args[2];
		AggregationEngine aggEng = new AggregationEngine(date, lookback, path);
		
		aggEng.flushToStaging();
		
		aggEng.checkLockMove();
		
	}	
	public AggregationEngine(String date, int lookback, String path) throws IOException{
		_fileMan = new FileManager(path);
		_quatAgg = new QuatlyAgg(_fileMan);
		_quatAgg.initAggregators(date, lookback);
		_cookAgg = new CookieAgg(_fileMan, _intMan);
		_intMan = new InterestManager(_fileMan);
		_intMan.loadCurrent();
		_fileMan.reloadFromSaveDir();
		Set<String> savedata = _fileMan.getAggPathSet(true);
		_quatAgg.loadFromSaveData(savedata);
		startUp(date , lookback);
		UpdateCookieAggWithSavedFilesandWrite();
	}
	void UpdateCookieAggWithSavedFilesandWrite() throws IOException
	{
		
		
		// load from CURRENT (=true)
		for(Integer relID : _intMan.pixset){
			Set<String> savedata = _fileMan.getAggPathByRelID(true, MyLogType.pixel, relID);
			_cookAgg.loadFromSaveData(savedata);
			//Util.pf("Found %d save data paths\n", savedata.size());
			_cookAgg.writeToStaging(MyLogType.pixel, relID);
			
		}
		for(Integer relID : _intMan.lineset){
			Set<String> savedata = _fileMan.getAggPathByRelID(true, MyLogType.imp, relID);
			_cookAgg.loadFromSaveData(savedata);
			for(MyLogType mlt : MyLogType.values()){
				_cookAgg.writeToStaging(mlt, relID);
			}
			//Util.pf("Found %d save data paths\n", savedata.size());
			
		}
		
		
	}
	
	void startUp(String date, int lookback)
	{
		MyLogType[] mlt = new MyLogType[] {MyLogType.click, MyLogType.imp, MyLogType.conversion, MyLogType.pixel};
		Set<String> pathset = _fileMan.newFilesLookBack(date, lookback, mlt);
		List<String> pathlist = new Vector<String>(pathset);
		Collections.shuffle(pathlist);
		
		Util.pf("Found %d new paths\n", pathset.size());
		
		for(int i = 0;i < pathlist.size() ; i++)
		{
			String onepath  = pathlist.get(i);
			processFile(onepath, 1);
			
			if((i % 100) == 0)
			{
				Util.pf("Done with file %d/%d\n", i, pathlist.size());
			}
			
			/*if(i >= 30)
				{ break; }*/
		}
		Object[] copyofbrokenfiles = brokenfiles.toArray();
		for(int i = 0;i < copyofbrokenfiles.length ; i++){
			String onepath  = (String)copyofbrokenfiles[i];
			Util.pf("last attempt for file : %s , if doesn't work it is ignored till next 15 minutes\n", onepath);
			processFile(onepath, 501);
			
		}
		brokenfiles.clear();
	}
	
	void flushToStaging() throws IOException
	{
		_quatAgg.writeToStaging();
		//_cookAgg.writeToStaging();
		_fileMan.flushCleanList();
	}
	

		
	void processFile(String filepath, int tryno)
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
					//Util.pf("Execution time for runforPixel "+(end-start)+" ms.\n");
					
				}
			else
			{
				PathInfo pinfo = new PathInfo(filepath);
				long start = System.currentTimeMillis();
				runForIcc(bread, pinfo.pType, pinfo.pVers);
				long end = System.currentTimeMillis();
				//Util.pf("Execution time for runforICC "+(end-start)+" ms.\n");
			}
			bread.close();
			_fileMan.reportFinished(filepath);
			
		} catch (IOException ioex) { 
			//Sleep 1 second and try 10 times
			if (tryno > 500){
				brokenfiles.add(filepath);
				return;

			}
			else{
				try {
					Thread.sleep(1000);
					Util.pf("Waiting 1 sec for file %s\n", filepath);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				processFile(filepath, ++tryno);
				
			}
		}		
		
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
			String jnkpath = _fileMan.getJunkPath();
			FileUtils.createDirForPath(jnkpath);
			File jnk = new File(jnkpath);
			File cur = new File(_fileMan.getBaseDir(true));
			File stg = new File(_fileMan.getBaseDir(false));
			
			// TODO: delete junk directories
			
			// Should be very fast operation
			boolean cur_renamed = cur.renameTo(jnk);
			if(!cur_renamed)
				Util.pf("problem rename to junk path : %s \n", jnk.getAbsoluteFile());
			boolean stg_renamed = stg.renameTo(cur);
			if(!stg_renamed)
				Util.pf("problem rename to current path\n");
			if(stg_renamed && cur_renamed)
				Util.pf("renamed directory %s --> %s\n", stg, cur);			
		}
		unlock();
	}	
	
	void lock() throws Exception
	{
		String dir = _fileMan.config.getDest();
		File otherslock = new File(dir + "/boss.lock");
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
		String dir = _fileMan.config.getDest();
		File ourlock = new File(dir +"/fastetl.lock");
		if(ourlock.exists()){
			ourlock.delete();
			return;
		}
		throw new Exception("there was supposed to be a lock here!");
	}		
}

