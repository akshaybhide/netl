
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;

// Handles the work of generating a feature report and learning a classifier.
public class LocalMode
{		
	public static final int NUM_THREADS = 6;
	
	public static final int LOCAL_SAVE_DAYS = 4;
			
	// Number of unique users per day
	public static final int UNIQ_SIZE = 5000;
	
	// Five minutes
	private static final long SLEEP_INC = 5*60*1000;
	
	private LookupPack _lookPack;
	
	String dayCode;
	
	private Map<CountryCode, WtpId> _wtpCutoffMap = Util.treemap();
	
	Set<String> pathSet = Util.treeset();
	LinkedList<String> pathQ = Util.linkedlist();
	
	Set<String> alphaSet = Util.treeset();
	Set<String> omegaSet = Util.treeset();
	
	Map<Integer, BufferedWriter> partWriteMap = Util.conchashmap();
	Map<String, BufferedWriter> listWriteMap = Util.conchashmap();

	IdHashPack idPack;
	
	private boolean _useBid = true; 
	private boolean _usePix = true;
	
	int partLineCount = 0;
	double startTime;
	
	int _numEx = 0;
	
	Integer _maxFile; 
	
	SimpleMail _logMail;
	
	public static void main(String[] args) throws Exception
	{
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		ArgMap amap = Util.getClArgMap(args);
		boolean oneday = amap.getBoolean("oneday", false);
		
		// Util.showMemoryInfo();
		// System.exit(1);
		
		if(oneday)
		{
			Util.pf("Running for single day %s\n", daycode);
			LocalMode lmode = new LocalMode(daycode, amap);	
			
			try {
				lmode.loadPaths();
				lmode.carefulDeleteOld();
				lmode.doIt(); 			
			} catch (Exception ex) {
				lmode._logMail.addExceptionData(ex);
			}
			
			lmode._logMail.send2admin();			
			
		} else {
			LocalModeDaemon lmd = new LocalModeDaemon(TimeUtil.dayAfter(daycode), amap);
			lmd.startDaemon();
		}
	}
	
	private static class LocalModeDaemon extends DailyDaemon
	{
		private ArgMap _argMap;
		
		LocalModeDaemon(String daycode, ArgMap amap)
		{
			super(daycode);
			_argMap = amap;
		}
		
		public String getShortStartTimeStamp()
		{
			return "02:00:00";	
		}
		
		public void runProcess()
		{
			LocalMode lmode = new LocalMode(getPrevDayCode(), _argMap);
			
			try {
				
				Util.pf("Running for daycode %s\n", getPrevDayCode());
				
				lmode.loadPaths();
				lmode.carefulDeleteOld();
				lmode.doIt(); 			
				
			} catch (Exception ex) {
				
				lmode._logMail.addExceptionData(ex);
			}
			
			lmode._logMail.send2admin();
		}
	}
	
	public LocalMode(String dc, ArgMap argmap)
	{
		dayCode = dc;	
		
		_maxFile = argmap.getInt("maxfile", Integer.MAX_VALUE);
		_usePix = argmap.getBoolean("usepix", true); 
		_useBid = argmap.getBoolean("usebid", true);			
		
		_logMail = new DayLogMail(this, dayCode);		
		
		
		_wtpCutoffMap = UserIndexUtil.getWtpCutoffMap();	
		
		_logMail.pf("Using cutoffmap  %s\n", _wtpCutoffMap);
		
		idPack = new IdHashPack(dayCode);
	}
	
	private static String getStartTimestamp(String daycode)
	{
		return Util.sprintf("%s 03:00:00", daycode);	
	}
	
	public Set<String> getPathSet()
	{
		return Collections.unmodifiableSet(pathSet);	
	}
	
	void doIt() throws IOException
	{
		// Rebuild to make sure we're not out of date.
		ListInfoManager.rebuildSing();		
		
		initLookup();
		initWriters();
		runScan();
		closeWriters();
		uploadResults();
	}
	
	void initLookup() throws IOException
	{
		Configuration myconf = new Configuration();
		String startday = UserIndexUtil.getBlockStartForDay(dayCode);
		_logMail.pf("Loading staging data from startday=%s for daycode=%s\n", startday, dayCode);		// Make sure the LookupPack is empty
		
		_lookPack = new LookupPack(startday);	
		{
			FileSystem fsys = FileSystem.get(new Configuration());
			while(!_lookPack.stagingInfoReady(fsys))
			{
				_logMail.pf("Staging Info not yet ready, waiting....\n");
				try { Thread.sleep(60*60*1000); } // sleep for an hour }
				catch (InterruptedException ex) { }
			}
			_lookPack.readFromHdfs(FileSystem.get(myconf));			
		}
		
		_logMail.pf("ListCountMap: %s\n", _lookPack.getListCountMap());

		// Show info about how much memory the LookupPack takes
		Util.showMemoryInfo(_logMail);
		
		_logMail.pf("Staging data Lookup took %d seconds\n", _lookPack.getLookupTimeSecs());	
	}
	
	void runScan() throws IOException
	{
		startTime = Util.curtime();
		
		for(int i = 0; i < NUM_THREADS; i++)
		{
			SubScan sscan = new SubScan(i);
			sscan.start();
		}
		
		while(!isComplete())
		{
			try { Thread.sleep(2000);	}
			catch (InterruptedException ex ) { }
			
			idPack.saveIfReady(_logMail);
		}
	}
	
	void closeWriters() throws IOException
	{
		// Write the unique user data
		
		for(BufferedWriter bwrite : partWriteMap.values())
		{
			bwrite.close();	
		}
		
		for(String list : listWriteMap.keySet())
		{
			listWriteMap.get(list).close();	
		}		
	}

	void uploadResults() throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		
		// Upload negative pool information
		idPack.writeData(fsys);
		
		// Upload partition files
		for(int i = 0; i < 24; i++)
		{	
			String partpath = partPathFromId(i, dayCode);
			Path src = new Path("file://" + partpath);
			
			String hdfspath = Util.sprintf("%s/part-%s.txt.gz", hdfsPartDir(dayCode), Util.padLeadingZeros(i, 5));
			Path dst = new Path(hdfspath);
			
			_logMail.pf("Uploading src/dst \n\t%s\n\t%s\n", partpath, hdfspath);
			fsys.copyFromLocalFile(src, dst);
		}
		
		for(String listcode : listWriteMap.keySet())
		{	
			String slicepath = listPathFromCode(listcode, dayCode);
			Path src = new Path("file://" + slicepath);
			
			String hdfspath = Util.sprintf("%s/%s.slice.gz", hdfsSliceDir(dayCode), listcode);
			Path dst = new Path(hdfspath);
			
			_logMail.pf("Uploading src/dst \n\t%s\n\t%s\n", slicepath, hdfspath);
			fsys.copyFromLocalFile(src, dst);
		}		
	}

	void carefulDeleteOld() throws IOException
	{
		Calendar gimp = TimeUtil.getYesterday();

		for(int i = 0; i < LOCAL_SAVE_DAYS; i++)
			{ gimp = TimeUtil.dayBefore(gimp); }
		
		String delday = TimeUtil.cal2DayCode(gimp);
		_logMail.pf("Going to careful-delete daycode %s\n", delday);
		
		FileSystem fsys = FileSystem.get(new Configuration());
		
		checkCleanDir(fsys, new File(localPartDir(delday)), new Path(hdfsPartDir(delday)));
		checkCleanDir(fsys, new File(localSliceDir(delday)), new Path(hdfsSliceDir(delday)));
	}

	void checkCleanDir(FileSystem fsys, File localdir, Path hdfsdir) throws IOException
	{
		if(!localdir.exists())
		{ 
			_logMail.pf("Local dir %s to clean does not exist\n", localdir.toString());
			return; 
		}
		
		for(File subfile : localdir.listFiles())
		{
			Path hdfsfile = new Path(hdfsdir + "/" + subfile.getName());
			HadoopUtil.checkUpDeleteLocal(fsys, subfile, hdfsfile);			 
		}
		
		// This will fail if the directory is nonempty
		boolean diddel = localdir.delete();
		
		if(diddel)
			{ _logMail.pf("Careful-Cleaned local directory %s\n", localdir);	}
		else 
			{ _logMail.pf("WARNING: local dir %s not deleted\n", localdir); }
	}
	
	synchronized void reportError(Exception ex)
	{
		if(_numEx < 3)
		{
			SimpleMail exmail = new SimpleMail("ERROR in LocalMode Slice");
			exmail.addExceptionData(ex);
			exmail.send2admin();
		}
		
		_numEx++;
	}
	
	synchronized void reportFinished(String path)
	{
		omegaSet.add(path);
		double totsecs = (Util.curtime()-startTime)/1000;
		
		String estcomplete = TimeUtil.getEstCompletedTime(omegaSet.size(), pathSet.size(), startTime);
		
		if((omegaSet.size() % 100) == 0 || isComplete())
		{
			_logMail.pf("Completed path %d / %d, average time %.03f, estcomplete=%s, total negpool saves %d \n", 
				omegaSet.size(), pathSet.size(), totsecs/omegaSet.size(), estcomplete, idPack.getTotalSaves());
			
		}
	}
	
	synchronized int numComplete() 
	{
		return omegaSet.size();	
	}
	
	synchronized boolean timeToQuit()
	{
		return pathQ.isEmpty();
	}
	
	synchronized boolean isComplete()
	{
		return timeToQuit() && (alphaSet.size() == omegaSet.size());
	}	
	
	synchronized String pollPath()
	{
		if(pathQ.isEmpty())
			{ return null; }
		
		String newpath = pathQ.poll();
		alphaSet.add(newpath);
		return newpath;
	}
	
	private void initWriters() throws IOException
	{
		for(int i = 0; i < UserIndexUtil.NUM_SHUF_PARTITIONS; i++)
		{	
			String partpath = partPathFromId(i, dayCode);
			FileUtils.createDirForPath(partpath);
			
			// BufferedWriter bwrite = new BufferedWriter(new FileWriter(partpath));
			BufferedWriter bwrite = FileUtils.getGzipWriter(partpath);
			
			partWriteMap.put(i, bwrite);
		} 
		
		for(String listcode : _lookPack.getListCodes())
		{
			String listpath = listPathFromCode(listcode, dayCode);
			FileUtils.createDirForPath(listpath);
			
			// BufferedWriter bwrite = new BufferedWriter(new FileWriter(listpath));	
			BufferedWriter bwrite = FileUtils.getGzipWriter(listpath);
			
			listWriteMap.put(listcode, bwrite);
		} 		
		
		//uniqsWriter = new BufferedWriter(new FileWriter(getUniqCandPath(dayCode)));
		
	}
	
	static String hdfsPartDir(String dc)
	{	
		return Util.sprintf("/userindex/sortscrub/%s/", dc); 
	}
	
	static String hdfsSliceDir(String dc)
	{	
		return Util.sprintf("/userindex/dbslice/%s/", dc);
	}	
	
	static String localPartDir(String dc)
	{
		return Util.sprintf("%s/BIGDATA/shuf/%s", UserIndexUtil.LOCAL_UINDEX_DIR, dc);
	}
	
	static String localSliceDir(String dc)
	{
		return Util.sprintf("%s/BIGDATA/slice/%s", UserIndexUtil.LOCAL_UINDEX_DIR, dc);
	}	
	
	static String partPathFromId(Integer id, String dc)
	{
		return Util.sprintf("%s/part-%s.%s", localPartDir(dc), Util.padLeadingZeros(id, 5), UserIndexUtil.PART_SUFF);	
	}
	
	static String listPathFromCode(String listcode, String dc)
	{
		return Util.sprintf("%s/%s.%s", localSliceDir(dc), listcode, UserIndexUtil.SLICE_SUFF);	
	}	
	
	void writeShufLine(WtpId wtpid, DataTypeCode dcode, String logline, CountryCode ccode) throws IOException
	{
		Util.massert(_wtpCutoffMap.containsKey(ccode), "Country Code %s not found in cutoff map");
		
		Util.massert(logline.toUpperCase().indexOf(ccode.toString()) > -1, "CCode %s not found in log line %s", ccode, logline);
		
		if(wtpid.compareTo(_wtpCutoffMap.get(ccode)) < 0)
		{
			int hc = UserIndexUtil.uniformWtpPartition(wtpid.toString());
			
			Writer partwrite = partWriteMap.get(hc);
			
			synchronized (partwrite) {
				
				partwrite.write(wtpid.toString());
				partwrite.write("\t");
				partwrite.write(dcode.toString());
				partwrite.write("\t");
				partwrite.write(logline);
				partwrite.write("\n");
				
				// partWriteList.get(hc).write(wtpid+"\t"+logline+"\n");
				// partwrite.write(wtpid+"\t"+logline+"\n");
			}
			
			//bUtil.pf("Going to write id %s\n", wtpid);	
			partLineCount++;
		}		
	}
	
	synchronized void writeSliceLine(String listcode, WtpId wid, DataTypeCode dcode, String logline) throws IOException
	{
		String outputkey = Util.sprintf("%s%s%s", listcode, Util.DUMB_SEP, wid);
		
		BufferedWriter bwrite = listWriteMap.get(listcode);
		
		synchronized (bwrite)
		{
			bwrite.write(outputkey);
			bwrite.write("\t");
			bwrite.write(dcode.toString());
			bwrite.write("\t");			
			bwrite.write(logline);
			bwrite.write("\n");
		}
		
		// Util.pf("Found hit for wid=%s, listcode=%s\n", wid, listcode);
		// listWriteMap.get(listcode).write(outputkey + "\t" + ble.getLogLine() + "\n");
	}	
	
	public void loadPaths()
	{
		// LogType[] touse = new LogType[] { LogType.no_bid_all, LogType.bid_all, LogType.bid_pre_filtered };
		
		if(_useBid)
		{
			LogType[] touse = new LogType[] { LogType.no_bid_all, LogType.bid_all  };
			
			for(ExcName oneexc : ExcName.values())
			{
				for(LogType onetype : touse)
				{
					List<String> plist = Util.getNfsLogPaths(oneexc, onetype, dayCode);
					if(plist == null)
						{ continue; }
					
					pathSet.addAll(plist);
					
					if(onetype == LogType.bid_pre_filtered)
					{
						Util.pf("Found %d paths for exchange %s for BIDPF\n", 
							plist.size(), oneexc);
					}
				}
			}
		}
				
		if(_usePix)
		{
			List<String> pixlist = Util.getNfsPixelLogPaths(dayCode);
			pathSet.addAll(pixlist);
			Util.pf("Found %d pixel log paths\n", pixlist.size());
		}
		
				
		List<String> subq = Util.vector();
		subq.addAll(pathSet);
		Random shufrand = new Random();
		shufrand.setSeed(1000);
		Collections.shuffle(subq, shufrand);
				
		for(int i = 0; i < _maxFile && i < subq.size(); i++)
			{ pathQ.add(subq.get(i)); }
	
		_logMail.pf("Paths loaded, found %d total, going to use %d\n", pathSet.size(), pathQ.size());
	}
	
	static class IdHashPack
	{
		private String _dayCode;
		
		private long _addSinceLastSave = 0;
		
		private int _totalSaves = 0;
		
		private static long ADDS_PER_SAVE = 50000000;
		
		// Country --> Pair<HashCode, Wtpid>
		// Map<String, SortedMap<Integer, String>> uniqPackMap = Util.treemap();
		// Use SortedSet<Pair<...>> instead of Map<...> because some IDs might get the same lottery ticket
		private Map<CountryCode, TreeSet<Pair<Integer, WtpId>>> _uniqPackMap = Util.treemap();		
		
		public IdHashPack(String dc)
		{
			_dayCode = dc;
			
			for(CountryCode ccode : UserIndexUtil.COUNTRY_CODES)
			{
				TreeSet<Pair<Integer, WtpId>> mset = Util.treeset();
				_uniqPackMap.put(ccode, mset);
			}	
		}
		
		int dayCtyHash(String wtpid, String cty)
		{
			// Classic bug: using the string combined the other way around,
			// the same wtpid would get nearly the same hash code regardless of the day!!!
			// String tohash = wtpid + cty + dayCode;
			
			String tohash = _dayCode + cty + wtpid;
			return tohash.hashCode();
		}
		
		// Add a potential candidate to the candidate pool list
		synchronized void reportUniqueCandidate(WtpId wtpid, CountryCode ccode)
		{			
			if(!_uniqPackMap.containsKey(ccode))
				{ return; }
			
			int hc = dayCtyHash(wtpid.toString(), ccode.toString());
			
			TreeSet<Pair<Integer, WtpId>> relset = _uniqPackMap.get(ccode);
			
			//if(relmap.size() < UNIQ_SIZE || hc < relmap.lastKey())
			if(relset.size() < UNIQ_SIZE || hc < relset.last()._1)
			{
				// Util.pf("Found new wtpid=%s with hashcode=%d\n", wtpid, hc);
				
				// if(relset.size() > 0)
				//	{ Util.pf("Relset size is %d, last is %d\n", relset.size(), relset.last()._1); }
				
				relset.add(Pair.build(hc, wtpid));
				
				while(relset.size() > UNIQ_SIZE)
				{
					relset.pollLast();
				}
			}
			
			_addSinceLastSave++;			
		}
		
		public int getTotalSaves()  { return _totalSaves; }
	
		// This is a periodic "backup" operation that protects the negpools
		// against a failure of the LocalMode scan or a failure of HDFS.
		// The files are small (~5k/country), so this is not going to cause a lot of pain.
		synchronized void saveIfReady(SimpleMail logmail) throws IOException
		{
			if(_addSinceLastSave < ADDS_PER_SAVE)
				{ return; }
			
			// if(logmail != null)
			//	{ logmail.pf("Add since last is %d, saving to local disk\n", _addSinceLastSave); }

			for(CountryCode ctry : _uniqPackMap.keySet())
			{
				String locnegpath = UserIndexUtil.getLocalNegPoolPath(_dayCode, ctry.toString());
				FileUtils.createDirForPath(locnegpath);
				BufferedWriter bwrite = FileUtils.getWriter(locnegpath);
				writeCtryData(ctry, bwrite);
				bwrite.close();
			}
			
			_addSinceLastSave = 0;
			_totalSaves++;
		}
		
		private Map<WtpId, Integer> revmap4ctry(CountryCode ccode)
		{
			Map<WtpId, Integer> revmap = Util.treemap();
			for(Pair<Integer, WtpId> onepair : _uniqPackMap.get(ccode))
			{ 
				// This is guaranteed because any given WTP ID should only get 
				// one lottery ticket per daycode per country code
				Util.massert(!revmap.containsKey(onepair._2));
				revmap.put(onepair._2, onepair._1); 
			}
			return revmap;			
		}
		
		private void writeCtryData(CountryCode ccode, Writer bwrite) throws IOException
		{
			Map<WtpId, Integer> revmap = revmap4ctry(ccode);
			for(WtpId wtpid : revmap.keySet())
			{
				bwrite.write(wtpid.toString());
				bwrite.write("\t");
				bwrite.write("" + revmap.get(wtpid));
				bwrite.write("\n");
			}
		}
		
		synchronized void writeData(FileSystem fsys) throws IOException
		{
			Util.pf("Writing IdPack data... ");
			
			//We'll also write the random pool here
			for(CountryCode ccode : _uniqPackMap.keySet())
			{
				Path poolpath = new Path(UserIndexUtil.getNegPoolPath(_dayCode, ccode));
				fsys.mkdirs(poolpath.getParent());
								
				PrintWriter bwrite = HadoopUtil.getHdfsWriter(fsys, poolpath);
				writeCtryData(ccode, bwrite);
				bwrite.close();
			}		
			Util.pf("... done\n");
		}
	}
	
	// Classic bug: was using just "country" here, but really want "user_country", arghh		
	// Classic bug #2, was using user_country for both, but need country for one
	// and user_country for the other!!!	
	
	private static CountryCode getCCodeOrNull(LogEntry logent)
	{		
		String cfield = logent.getField(LogField.user_country);
		
		try { return CountryCode.valueOf(cfield.toUpperCase()); }
		catch (Exception ioex ) { return null; }
	}
	
	private class SubScan extends Thread
	{
		int threadId; 
		
		Set<CountryCode> _ctySet;
		
		SubScan(int tid)
		{
			threadId = tid; 
			
			initCtySet();
		}
		
		void initCtySet()
		{
			_ctySet = Util.treeset();
			
			_ctySet.addAll(UserIndexUtil.COUNTRY_CODES);
		}
		
		public void run()
		{
			Util.pf("starting thread %d\n", threadId);

			while(true) 
			{
				if(timeToQuit())
				{
					Util.pf("Thread %d completed, terminating\n", threadId);	
					return;
				}
				
				String nextpath = pollPath();
				
				// It doesn't really matter if we miss a file. 
				// Could imagine some kind of retry-later process, but it's
				// just not that important.
				try { processFile(nextpath); }
				catch (Exception ex) 
				{ reportError(ex); } 
				
				// What's really important is that we report that we're finished,
				// Otherwise the whole process will hang
				reportFinished(nextpath);
			}
		}
			
		private double processFile(String nfslogpath) throws IOException
		{
			double startup = Util.curtime();
			
			if(nfslogpath.indexOf("pixel") > -1)
				{ processPixelFile(nfslogpath); }
			else 
				{ processBidLogFile(nfslogpath); }
			
			return (Util.curtime()-startup)/1000;			
		}	
		
		private void processPixelFile(String nfslogpath) throws IOException
		{
			// DumbReader bread = new DumbReader(nfslogpath);
			MyBufReader bread = new MyBufReader(nfslogpath);
						
			for(String logline = bread.readLine(); logline != null; logline = bread.readLine())
			{
				try {
					PixelLogEntry ple = PixelLogEntry.getOrNull(logline);
					if(ple == null)
						{ continue; }
					
					String wtpstr = ple.getField(LogField.wtp_user_id);
					WtpId wid = WtpId.getOrNull(wtpstr);					
					if(wid == null)
						{ continue; }
					
					CountryCode ccode = getCCodeOrNull(ple);
					if(ccode == null || !_ctySet.contains(ccode))
						{ continue; }
					
					idPack.reportUniqueCandidate(wid, ccode);				
				
					writeShufLine(wid, DataTypeCode.pix, logline, ccode);
					
					for(String hitcode : _lookPack.lookupId(wid))
						{ writeSliceLine(hitcode, wid, DataTypeCode.pix, logline); }
					
				} catch (Exception ex ) {
					_logMail.pf("Got exception %s", ex.getMessage());
				}
				
				// Util.pf("WTP id is %s, sub is %s\n", wtpid, wtpid.substring(0, 2));
			}
			
			
			// Util.pf("Wrote %d lines out of %d total\n", wcount, ltotal);
			bread.close();
		}		
		
		private void processBidLogFile(String nfslogpath) throws IOException
		{
			// DumbReader bread = new DumbReader(nfslogpath);
			MyBufReader bread = new MyBufReader(nfslogpath);
			PathInfo pinfo = new PathInfo(nfslogpath);
			
			for(String logline = bread.readLine(); logline != null; logline = bread.readLine())
			{
				try {
					
					BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, logline);
					if(ble == null)
						{ continue; }
					
					BidLogEntry minimal = ble.transformToVersion(LogType.UIndexMinType, LogVersion.UIndexMinVers2);
					
					WtpId wtpid = WtpId.getOrNull(minimal.getField(LogField.wtp_user_id));
					if(wtpid == null)
						{ continue; } 
					
					CountryCode ccode = getCCodeOrNull(minimal);
					if(ccode == null || !_ctySet.contains(ccode))
						{ continue; }				
					
					idPack.reportUniqueCandidate(wtpid, ccode);			
				
					writeShufLine(wtpid, DataTypeCode.bid, minimal.getLogLine(), ccode);
					
					for(String hitcode : _lookPack.lookupId(wtpid))
						{ writeSliceLine(hitcode, wtpid, DataTypeCode.bid, minimal.getLogLine()); }
					
				} catch (Exception ex ) {
					
					throw new RuntimeException(ex);
					
					// _logMail.pf("Got exception %s", ex.getMessage());
				}
				
				// Util.pf("WTP id is %s, sub is %s\n", wtpid, wtpid.substring(0, 2));
			}
			
			// Util.pf("Wrote %d lines out of %d total\n", wcount, ltotal);
			bread.close();
		}
	}
	
	
	private static class MyBufReader
	{
		private static final int QSIZE = 10000;
		
		BufferedReader bread;		
		
		LinkedList<String> lineQ = Util.linkedlist();
		
		boolean finished = false;
		
		public MyBufReader(String nfspath) throws IOException
		{
			bread = Util.getGzipReader(nfspath);
		}
		
		public String readLine() throws IOException
		{
			if(lineQ.isEmpty())
			{
				refQ();	
			}
			
			return lineQ.isEmpty() ? null : lineQ.poll();
		}
		
		void refQ() throws IOException
		{
			Util.massert(lineQ.isEmpty());
			
			for(int i = 0; i < QSIZE; i++)
			{
				String oneline = bread.readLine();
				
				if(oneline == null)
					{ break; } 
				
				lineQ.add(oneline);
			}
		}
		
		public void close() throws IOException
		{
			bread.close();	
		}		
		
	}
	
	private static class DumbReader
	{
		BufferedReader bread;
		
		public DumbReader(String nfspath) throws IOException
		{
			bread = Util.getGzipReader(nfspath);
		}
		
		public String readLine() throws IOException
		{
			return bread.readLine();	
		}
		
		public void close() throws IOException
		{
			bread.close();	
		}
	}	
	
}
