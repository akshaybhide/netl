
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.data_management.SegmentPathMan.*;

public class BluekaiDataMan
{
	public enum BluekaiEnum { LookupTimeMillis };
	
	private String _dayCode;
	
	private TreeMap<WtpId, String> _oneDayMap = Util.treemap();
		
	// public static final String BLUEKAI_RESOURCE_PATH = "/com/adnetik/resources/BluekaiTaxonomy.txt";
	public static final String BLUEKAI_RESOURCE_PATH_INFO = "/com/adnetik/resources/BluekaiUsedFeatureTaxonomy.tsv";

	
	public static SegmentPathMan BK_PATH_MAN = new SegmentPathMan(SegmentPathMan.Party3Type.bluekai, false);

	private static BlueUserQueue _SING_QUEUE;
	
	private static Map<Integer, String> _SEG2NAME; 
	
	public static void main(String[] args) throws IOException
	{
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		Map<String, String> optargs = Util.getClArgMap(args);
		Integer maxfile = optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : Integer.MAX_VALUE;
		
		MergeDaemon mdaemon = new MergeDaemon(TimeUtil.dayAfter(daycode), maxfile);
		mdaemon.startDaemon();
	}
	
	WtpId getIdFromLine(String oneline)
	{
		String[] toks = oneline.split("\t");
		return WtpId.getOrNull(toks[12]);
	}

	
	public static void setSingQ(String daycode) throws IOException
	{
		setSingQ(getHdfsSingQ(daycode));
	}
	
	public static void setLocalSingQ(String daycode) throws IOException
	{
		throw new RuntimeException("NYI");
		
	}
	
	public static synchronized boolean isQReady()
	{
		return (_SING_QUEUE != null);
	}
	
	public static void setSingQ(BlueUserQueue myqueue) throws IOException
	{
		Util.massert(_SING_QUEUE == null, "BlueKai Queue already initialized");
		_SING_QUEUE = myqueue;
	}
	
	public static synchronized void unsetSingQ()
	{
		_SING_QUEUE = null;
	}
	
	public static synchronized BlueUserQueue getSingQ() throws IOException
	{
		Util.massert(_SING_QUEUE != null, "Must set the Queue first!!");
		return _SING_QUEUE;
	}

	public static BlueUserQueue getHdfsSingQ(String daycode) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, BK_PATH_MAN.getHdfsMasterPath(daycode));
		return new BlueUserQueue(FileUtils.bufRead2Line(bread));
	}	
	
	public static BlueUserQueue getLocalSingQ(String daycode) throws IOException
	{
		BufferedReader bread = FileUtils.getReader(BK_PATH_MAN.getLocalMasterPath(daycode));
		return new BlueUserQueue(FileUtils.bufRead2Line(bread));
	}
	
	public static synchronized void resetSingQ(BlueUserQueue myqueue) throws IOException
	{
		_SING_QUEUE = myqueue;	
	}
	
	public static SortedSet<String> getNfsLogPaths(String daycode)
	{
		SortedSet<String> logset = Util.treeset();
		File nfsdir = new File(Util.sprintf("/mnt/adnetik/adnetik-uservervillage/prod/userver_log/bk_data/%s", daycode));
		
		Util.massert(nfsdir.exists() && nfsdir.isDirectory(), "Problem with NFS dir");
		
		for(File onepath : nfsdir.listFiles())
			{ logset.add(onepath.getAbsolutePath()); }

		return logset;
	}
	
	public static TaxonomyInfo getTaxonomy()
	{
		return TaxonomyInfo.getSing();
	}
	
	public static class MergeDaemon extends DailyDaemon
	{
		private int _maxFile;
		
		public MergeDaemon(String dc, int maxfile)
		{
			super(dc);	
			_maxFile = maxfile;
		}
		
		protected  String getShortStartTimeStamp()
		{ 
			return "08:00:00"; 
		}
		
		public void runProcess()
		{
			try {
				MergeOperation merge_op = new MergeOperation(getPrevDayCode(), _maxFile);
				merge_op.runOp();
			} catch (IOException ioex) {
				throw new RuntimeException(ioex);	
			}
		}
	}
	
	public static class TaxonomyInfo
	{
		private Set<Integer> _usedFeat = Util.treeset();
		private Map<Integer, String> _featNameMap = Util.conchashmap();
		
		private static TaxonomyInfo _SING; 
		
		private TaxonomyInfo()
		{
			initData();
		}

		// call getTaxonomy 
		private static synchronized TaxonomyInfo getSing()
		{
			if(_SING == null)
				{ _SING = new TaxonomyInfo(); }
			
			return _SING;
		}
		
		public Set<Integer> getFeatIdSet()
		{
			return Collections.unmodifiableSet(_usedFeat);
		}
		
		public String getFeatName(int nodeid)
		{
			return _featNameMap.get(nodeid);	
		}
		
		private void initData()
		{
			InputStream resource = (new SharedResource()).getClass().getResourceAsStream(BLUEKAI_RESOURCE_PATH_INFO);
			Scanner sc = new Scanner(resource, "UTF-8");
				
			while(sc.hasNextLine())
			{
				String s = sc.nextLine().trim();
				if(s.length() == 0)
					{ continue; }
				
				// Util.pf("resource line is %s\n", s);
				
				String[] toks = s.split("\t");
				
				if(toks.length != 3)
					{ continue; }
				
				String fname = toks[0];
				int nodeid = Integer.valueOf(toks[1]);
				boolean useme = (Integer.valueOf(toks[2]) == 1);
				
				if(useme)
					{ _usedFeat.add(nodeid); }
				
				_featNameMap.put(nodeid, fname);
			}			
			sc.close();
		}
	}
	
	public static class MergeOperation
	{
		private String _dayCode;
		private Integer _maxFile;
		
		private int _slurpLines = 0;
		private int _newUsers = 0;
		private int _newTotal = 0;				
		private int _updateUsers = 0;
		private int _noUpdateUsers = 0;
		private int _badWtpIds = 0;
				
		// private TreeMap<String, List<String>> _oneDayMap = Util.treemap();
		private SortedFileMap _sortFileMap;
		
		private SimpleMail _logMail;
		
		public MergeOperation(String dc)
		{
			this(dc, Integer.MAX_VALUE);
		}
		
		public MergeOperation(String dc, Integer mf)
		{
			_dayCode = dc;
			_maxFile = mf;
			_logMail = new SimpleMail(Util.sprintf("BlueKaiMergeOp for %s -> %s", TimeUtil.dayBefore(dc), dc));
		}
		
		private String getSlurpPath()
		{
			return Util.sprintf("%s/slurp/slurp_%s.txt", BK_PATH_MAN.getLocalDir(), _dayCode);
		}
		
		public void runOp() throws IOException
		{
			// DO this check early
			if(prevDataOkay())
			{
				double startup = Util.curtime();

				preprocessData();
				buildSortMap();
				
				double midpoint = Util.curtime();
				merge();
				double fintime = Util.curtime();
				_logMail.pf("Finished BlueKai Merge, slurp took %.03f, merge took %.03f, total %.03f\n",
					(midpoint-startup)/1000, (fintime-midpoint)/1000, (fintime-startup)/1000);
				
				finishUp();				
			}
			
			_logMail.send2admin();
		}
		
		private void finishUp() throws IOException
		{
			BK_PATH_MAN.renameGimp2Master(_dayCode);
			BK_PATH_MAN.uploadMaster(_dayCode, _logMail);
			BK_PATH_MAN.deleteOldLocalMaster(_dayCode, 1, _logMail);
			
			// Delete the temp file
			{
				File slurpfile = new File(getSlurpPath());	
				
				if(slurpfile.exists())
				{  
					slurpfile.delete(); 
					_logMail.pf("Deleted slurp file %s\n", slurpfile.getAbsolutePath());
				}
			}
		}
		
		// Check that the previous day's Master list is available, and the write path does not exist
		private boolean prevDataOkay() throws IOException
		{
			File localmaster = new File(BK_PATH_MAN.getLocalMasterPath(TimeUtil.dayBefore(_dayCode)));
			
			if(!localmaster.exists())
			{
				_logMail.pf("Previous master list %s does not exist, aborting\n", localmaster.getAbsolutePath());
				return false;
			}
			
			return true;
		}
		
		private void preprocessData() throws IOException
		{
			SortedSet<String> nfspaths = getNfsLogPaths(_dayCode);
			
			int tcount = 0;
			_logMail.pf("Slurping %d nfs log files\n", nfspaths.size());

			BufferedWriter slurpwrite = FileUtils.getWriter(getSlurpPath());
			for(String onepath : nfspaths)
			{
				Util.pf(".");
				
				preprocessFile(onepath, slurpwrite);

				if(tcount++ > _maxFile)
					{ break; }
			}
			slurpwrite.close();
			
			_logMail.pf("Finished slurp, read %d files and %d lines total, %d bad WTPs\n", 
				tcount, _slurpLines, _badWtpIds);			
		}
		
		private void preprocessFile(String onepath, Writer slurpwrite) throws IOException
		{
			BufferedReader bread = Util.getGzipReader(onepath);
			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				String[] toks = oneline.split("\t");
				// Util.pf("WTP is %s\n", toks[12]);
				WtpId wid = WtpId.getOrNull(toks[12]);
				if(wid  == null)
				{
					_badWtpIds++;
					// Util.pf("Invalid WTP: %s\n", toks[12]);
					continue;
				}

				// Okay, some of the BK records come in with daycodes that are not the same 
				// as the NFS date - the same spillover issue. I am going to just use the NFS 
				// date, to make QA easier.
				/* 
				String[] date_time = toks[0].split(" ");
				String justdate = date_time[0];
				if(!TimeUtil.checkDayCode(justdate))
					{ continue; }
					
				*/
						
				// Check that the segment info string is okay
				if(!checkSegInfo(toks[11]))
					{ continue; }
				
				// Write a line of data.
				// Format is <WTP, DATE, SEGINFO>
				slurpwrite.write(toks[12].toLowerCase()); // WTP id
				slurpwrite.write("\t");
				slurpwrite.write(_dayCode); // date
				slurpwrite.write("\t");
				slurpwrite.write(toks[11]); // seginfo string
				slurpwrite.write("\n");
				
				_slurpLines++;
			}			
			bread.close();			
		}
		
		private boolean checkSegInfo(String segstr)
		{
			String[] segtoks = segstr.split(",");	
			
			for(String oneseg : segtoks)
			{
				try { Integer x = Integer.valueOf(oneseg); }
				catch (NumberFormatException nfex)  { return false; }
			}
			
			return true;
		}
		
		private void buildSortMap() throws IOException
		{
			// Going to do the sort myself, instead of using SortMapFile
			_logMail.pf("Preparing to sort slurpfile...");
			Util.unixsort(getSlurpPath(), "");
			_logMail.pf("... done");
			
			// dosort=false
			_sortFileMap = SortedFileMap.buildFromFile(new File(getSlurpPath()), false);
		}
		
		void merge() throws IOException
		{
			double startup = Util.curtime();
			_logMail.pf("Starting merge operation...\n");
			String dropcutoff = TimeUtil.nDaysBefore(_dayCode, 60);
			
			BufferedWriter pwrite = FileUtils.getWriter(BK_PATH_MAN.getGimpPath(_dayCode));
			
			int writecount = 0;
			BlueUserQueue bqueue = getLocalSingQ(TimeUtil.dayBefore(_dayCode));

			while(bqueue.hasNext())
			{
				BluserPack bupnext = bqueue.nextPack();

				while(!_sortFileMap.isEmpty() && _sortFileMap.firstKey().compareTo(bupnext.wtpid) < 0)
				{
					// This is a new WTP id that is not in the Master file, and was found for the first time today
					Map.Entry<String, List<String>> ent = _sortFileMap.pollFirstEntry();

					BluserPack bupnew = new BluserPack(ent.getKey());
					bupnew.integrateShortPixelLines(ent.getValue());
					writecount += bupnew.write(pwrite, dropcutoff);
					_newUsers++;
					_newTotal++;
				}
				
				if(!_sortFileMap.isEmpty() && _sortFileMap.firstKey().equals(bupnext.wtpid))
				{
					// Here there is data in both the Master file and the day's slurped data
					Map.Entry<String, List<String>> ent = _sortFileMap.pollFirstEntry();
					bupnext.integrateShortPixelLines(ent.getValue());
					_updateUsers++;
					
				} else {
					_noUpdateUsers++;	
				}
				
				_newTotal++;
				writecount += bupnext.write(pwrite, dropcutoff);
				
				if((bqueue.polledUsers % 10000) == 0)
				{
					Util.pf(".");
					//double userpersec = bqueue.polledUsers /((Util.curtime()-startup)/1000);
					//Util.pf("Finished with %d users, %.03f users per second\n",
					//	bqueue.polledUsers, userpersec);
				}
			}
			
			while(!_sortFileMap.isEmpty())
			{
				// Write out remaining data.
				Map.Entry<String, List<String>> ent = _sortFileMap.pollFirstEntry();
				BluserPack bupnew = new BluserPack(ent.getKey());
				bupnew.integrateShortPixelLines(ent.getValue());
				
				// TODO: this doesn't inform us about whether or not a user actually has any data associated with him
				writecount += bupnew.write(pwrite, dropcutoff);	
				_newTotal++;				
			}
						
			pwrite.close();
			
			_logMail.pf("Finished merge, stats: \n\t%d updated users\n\t%d non-updated\n\t%d prev total\n\t%d new total\n",
				_updateUsers, _noUpdateUsers, bqueue.polledUsers, _newTotal);
			
			_logMail.pf("Master file size: \n%d lines before\n%dlines after\n", bqueue.linesRead, writecount);
			
		}		
	}
	
	public static class BlueUserQueue extends SegmentDataQueue
	{		
		private boolean _isNullMode = false;
		
		public BlueUserQueue(LineReader bread) throws IOException
		{
			super(Party3Type.bluekai, bread);
		}		
		
		public void setNullMode()
		{
			_isNullMode = true;
		}

		public BluserPack lookup(String userid) throws IOException
		{
			if(_isNullMode)
				{ return null; }
			
			Map.Entry<String, List<String>> myentry = lookupEntry(userid);
			return (myentry == null ? null : BluserPack.build(myentry));
		}

		public BluserPack nextPack() throws IOException
		{
			Util.massert(!_isNullMode, "Attempt to call nextPack(..) in Null Mode");
			return BluserPack.build(nextEntry());
		}	
		
		public BluserPack buildEmpty(String wtpid)
		{
			return new BluserPack(wtpid);
		}
	}
	
	public static class BluserPack extends SegmentPack.IntPack
	{
		
		BluserPack(String wtpid)
		{
			super(wtpid);	
		}
		
		BluserPack(Map.Entry<String, List<String>> myentry)
		{
			super(myentry);
		}
			
		private static BluserPack build(Map.Entry<String, List<String>> myentry)
		{
			return new BluserPack(myentry);
		}
		
		void integrateShortPixelLines(Collection<String> shortlines)
		{
			for(String oneline : shortlines)
			{
				// Okay, the SHORT lines are just <wtp, daycode, seginfolist>
				try {
					String[] s_toks = oneline.split("\t");	
					
					String justdate = s_toks[1];
					Util.massert(TimeUtil.checkDayCode(justdate), "Invalid daycode %s, need to error check elsewhere", justdate);
					
					// Util.pf("Date is %s, user %s has %d segments \n", date_time[0], wtpid, _segData.size());
					
					String[] seginfo = s_toks[2].split(",");
					
					//Util.pf("Found %d segs for user=%s, daycode=%s, seginfo=%s\n",
					//	seginfo.length, wtpid, justdate, s_toks[1]);
					
					for(String oneseg : seginfo)
					{
						Integer segid = Integer.valueOf(oneseg);
						String prevday = _segData.get(segid);
						String newday = (prevday == null || prevday.compareTo(justdate) < 0) ? justdate : prevday;
						_segData.put(segid, newday);
					}
					
				} catch (Exception ex) {
					
					// TODO: this doesn't help anything... 
					ex.printStackTrace();	
				}
			}		
		}
		
		@Override
		void integrateNewData(Collection<String> shortlines, String daycode)
		{
			integrateShortPixelLines(shortlines);
		}
	}
}
