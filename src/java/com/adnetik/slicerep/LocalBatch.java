
package com.adnetik.slicerep;

import java.util.*;
import java.io.*;
import java.sql.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.BidLogEntry.*;


public class LocalBatch
{
	public static final int NUM_THREADS = 6;
	
	String _dayCode;
	
	Set<Integer> _campIdSet = Util.treeset();
	
	int outputLine = 0;
	int procLine = 0;
	
	ReporterPack _reportPack;
	
	PathController _pControl;
	
	BatchStats _bStats;
	
	ClickChecker _clickChecker;
	
	private int _failClicks = 0;
	private int _okayClicks = 0;
	
	public LocalBatch(String dc, Collection<String> plist, ClickChecker ccheck)
	{	
		_dayCode = dc;
			
		CatalogUtil.initSing(null, DbTarget.internal);		

		_pControl = new PathController(plist);
		_bStats = new BatchStats();

		_reportPack = new ReporterPack.MemoryPack(CatalogUtil.getSing().getDimSetMap());
		// _reportPack = new ReporterPack.ShardPack(_dimMap);

		Util.pf("Finished initializing pathcontroller, now querying for dim/fact-maps...\n");
		
		_clickChecker = ccheck;
	}	
	
	
	public void doIt() throws IOException
	{
		double startall = Util.curtime();
		processData();
		
		_bStats.logMemInfo();
		double procfin = Util.curtime();
		
		sendData();
		
		_bStats.setTimeSecs("totaltime", (Util.curtime()-startall));
		_bStats.setTimeSecs("aggtime", (procfin-startall));
		_bStats.setTimeSecs("uploadtime", (Util.curtime()-procfin));
		
		_bStats.setField("totalpreagg", _pControl._totalPreAggLines);
		_bStats.setTimeSecs("avgtimeperfile", (Util.curtime()-startall)/_pControl.numComplete());
		_bStats.setField("numfiles", ""+_pControl.numComplete());
		
		_bStats.setField("failclicks", ""+_failClicks);
		_bStats.setField("okayclicks", ""+_okayClicks);
	}

	public void writeLogData(String writepath)
	{
		_bStats.writeLogData(writepath);
	}
	
	private void processData() throws IOException
	{		
		
		/*
		String badpath = "/mnt/adnetik/adnetik-uservervillage/dbh/userver_log/imp/2013-04-10/2013-04-10-09-15-05.EDT.imp_v22.dbh-rtb-california2_8b82e.log.gz";
		SubScan sscan =  new SubScan(0);
		try { sscan.processFile(badpath); }
		catch (Exception ex) {
			
			ex.printStackTrace();
			throw new RuntimeException(ex);
		}
		*/
		
		for(int i = 0; i < NUM_THREADS; i++)
		{
			SubScan sscan = new SubScan(i);
			sscan.start();
		}
		
		while(!_pControl.isComplete())
		{
			try { Thread.sleep(2000);	}
			catch (InterruptedException ex ) { }
		}

		_reportPack.close();
	}		
	
	private void sendData() throws IOException
	{
		SortedSet<Integer> distqt = Util.treeset();
		
		for(AggType atype : AggType.values())
		{
			// Double Copy 
			// Copying to both Thorin-internal and new DB machine
			// TODO: when machines are in sync, take this out
			SliUtil.UploaderInterface kvup;
			{
				// TargetStaging=true
				// KvUploader kv_a = new KvUploader(atype, _dayCode, true, DbTarget.new_internal);	
				KvUploader kv_b = new KvUploader(atype, _dayCode, true, DbTarget.internal);
				// kvup = new SliUtil.UploaderList(kv_a, kv_b);
				kvup = new SliUtil.UploaderList(kv_b);
			}
			// SliUtil.UploaderInterface kvup = new KvUploader(atype, _dayCode, true);

			_reportPack.send2KvUploader(atype, kvup);
			
			// These should be the same, if not there is a glitch in the LOAD DATA INFILE command
			_bStats.setField("postagg_in_" + atype.toString(), kvup.getTotalIn());
			_bStats.setField("postagg_up_" + atype.toString(), kvup.getTotalUp());
			
			// distqt.add(kvup._quarterSet.size());
			
			/*
			// Also send to staging area 
			{
				KvUploader kvstage = new KvUploader(atype, _dayCode);
				kvstage.setTargetStaging(true);
				_reportPack.send2KvUploader(atype, kvstage);				
			}
			*/
		}
	}
	
	private synchronized void reportException(String onepath, Exception ex)
	{
		Util.pf("Fatal exception hit for path %s\n", onepath);		
		ex.printStackTrace();
		System.exit(1);
	}

	// TODO: these need to have LOCAL COPIES of all the relevant data they are using
	private class SubScan extends Thread
	{
		int threadId; 
			
		SubScan(int tid)
		{
			threadId = tid; 
		}
		
		public void run()
		{
			Util.pf("starting thread %d\n", threadId);

			while(true) 
			{					
				String nextpath = _pControl.pollPath();
				if(nextpath == null)
				{
					Util.pf("Thread %d finished\n", threadId);	
					break;
				}		
				
				int rows = -1;
				
				try { rows = processFile(nextpath); }
				catch (Exception ioex) {
					
					// TODO: really need to figure out what to do here. 
					// If one of the threads fails to call reportFinished(), the whole
					// process stalls.
					//ioex.printStackTrace();	
					// System.exit(1);
					reportException(nextpath, ioex);
					throw new RuntimeException(ioex);
				}
				
				_pControl.reportFinished(nextpath, rows);
			}
		}
			
		private int processFile(String onepath) throws Exception
		{
			PathInfo pinfo = new PathInfo(onepath);
			BufferedReader bread = null;
			
			// 6 seconds * 500 tries = 50 minutes
			for(int i = 0; i < 500; i++)
			{
				try { bread = Util.getGzipReader(onepath); }
				catch (IOException ioex) { }
				
				if(bread != null)
					{ break; }
				
				Util.pf("Failed to open file, retrying... \n");
				Thread.sleep(6000);
			}
			
			// No clue what to do here, but I think the retry strategy will eventually work
			if(bread == null)
				{ return 0; }
			
			int procrows = 0; // number of rows in file
			
			// Util.pf("PINfo is %s\n", pinfo);
			// Util.pf("Path is %s\n", onepath);
			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				procrows++;
								
				try {
					BleStructure blestruct = BleStructure.buildStructure(pinfo.pType, pinfo.pVers, oneline);
					
					if(pinfo.pType == LogType.click)
					{
						if(!_clickChecker.greenLight(blestruct.getLogEntry()))
						{ 
							_failClicks++;
							continue; 
						} 
						
						// Now add the BLE to the data set for the click checker
						_clickChecker.add2dataSet(blestruct.getLogEntry());
						_okayClicks++;
					}
									
					DumbDimAgg dagg;
					Metrics magg;
					
					
					
					// All of this stuff is making queries against the global Catalog object
					// TODO: worry more about how to do this 
					// synchronized (CatalogUtil.getSing())
					{
						dagg = new DumbDimAgg(blestruct.getLogEntry());
						magg = blestruct.returnMetrics();	
												
						// ONLY need to do the conversion to standardize 
						// This actually doesn't depend on CatalogUtil
						magg.standardizeCostInfo(pinfo);	
					}
					
					for(AggType atype : AggType.values())
					{						
						_reportPack.writeKv(atype, dagg, magg);
					}
				} catch (BidLogFormatException blex) {
					
					Util.pf("Bid log format exception %s\n", blex.getMessage());
					
				} /* catch (Exception ex) {
					
					// TODO: what to do here?
					// Util.pf("Bid log format exception %s\n", ex.getMessage());
					ex.printStackTrace();
					throw new RuntimeException(ex);

				}	*/		
			} 
			bread.close();
			
			
			return procrows;
		}		
	}	
	
	class PathController
	{
		LinkedList<String> _pathQ = Util.linkedlist();
		
		Set<String> _strtSet = Util.treeset(); // List of paths that have been STARTED
		Set<String> _compSet = Util.treeset(); // List of paths that have been COMPLETED
		
		int totalSize;
		
		int _totalPreAggLines = 0;
		
		Long _startTime = null;
		
		public PathController(Collection<String> pathlist)
		{
			_pathQ.addAll(pathlist);
			totalSize = _pathQ.size();
		}
		
		synchronized void reportFinished(String path, int rowsinfile)
		{
			_compSet.add(path);
			double totsecs = (Util.curtime()-_startTime)/1000;
			
			if((_compSet.size() % 100) == 0 || isComplete())
			{
				Util.pf("Completed path %d / %d, average time %.03f\n", _compSet.size(), totalSize, totsecs/_compSet.size());
			}
			
			_totalPreAggLines += rowsinfile;
		}
		
		synchronized int numComplete() 
		{
			return _compSet.size();
		}
		
		synchronized boolean isComplete()
		{
			return (_pathQ.isEmpty() && (_strtSet.size() == _compSet.size()));
		}	
		
		synchronized String pollPath()
		{
			// Start the timer when the first pollPath() request comes in 
			if(_startTime == null)
				{ _startTime = Util.curtime(); }
			
			if(_pathQ.isEmpty())
				{ return null; }
			
			String newpath = _pathQ.poll();
			_strtSet.add(newpath);
			return newpath;
		}
	}

	static class BatchStats
	{
		Map<String, String> statmap = Util.linkedhashmap();
		
		public void setField(String fname, String val)
		{
			statmap.put(fname, val);	
		}
		
		public void setField(String fname, int val)
		{
			statmap.put(fname, ""+val);
		}
		
		public void setField(String fname, double val)
		{
			statmap.put(fname, Util.sprintf("%.04f", val));	
		}
		
		public void setTimeSecs(String fname, double millival)
		{
			setField(fname, millival/1000);	
		}
		
		// Call this at point of PEAK memory usage.
		public void logMemInfo()
		{
			Runtime rt = Runtime.getRuntime();
			
			setField("usedmemory", Util.commify(rt.totalMemory() - rt.freeMemory()));
			setField("freememory", Util.commify(rt.freeMemory()));
			setField("totalmemory", Util.commify(rt.totalMemory()));
		}
		
		void writeLogData(String writepath)
		{
			List<String> loglist = Util.vector();
			
			for(String key : statmap.keySet())
			{
				loglist.add(Util.sprintf("%s\t%s", key, statmap.get(key)));	
			}
			
			FileUtils.createDirForPath(writepath);
			FileUtils.writeFileLinesE(loglist, writepath);
		}
	}
}
