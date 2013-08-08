
package com.adnetik.slicerep;

import java.util.*;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.BidLogEntry.*;


public abstract class ControlTable
{
	
	protected Integer fallBehind;
	
	protected abstract List<String> nextBatch(int lookback);
	
	// TODO: switch this back to protected
	public abstract void reportFinished(Collection<String> compset);
	
	protected abstract boolean finishedForDay(String daycode);
	
	public abstract void printStatusInfo(); 
	
	public SortedSet<String> nextBatch(int lookback, int maxfiles)
	{
		Comparator<String> simplenamecomp = new Comparator<String>() 
		{
			public int compare(String a, String b)
			{
				return SliUtil.pathSimpleName(a).compareTo(SliUtil.pathSimpleName(b));
			}
		};
		
		List<String> fullbatch = nextBatch(lookback);
		
		TreeSet<String> smartset = new TreeSet<String>(simplenamecomp);
		smartset.addAll(fullbatch);
		
		while(smartset.size() > maxfiles)
			{ smartset.pollLast(); }
		
		Util.pf("Smartset size is %d, fullbatch is %d for %d days lookback\n", 
			smartset.size(), fullbatch.size(), lookback);
		
		fallBehind = fullbatch.size() - smartset.size();
		
		return smartset;
	}
	
	public int getFallBehind() 
	{
		return fallBehind;	
	}
	
	public static class CleanListImpl extends ControlTable
	{
		TreeMap<String, Set<String>> _cleanMap = Util.treemap();
		String _cleanDir;
		
		public CleanListImpl(String cdir)
		{
			_cleanDir = cdir;
			
			File probe = new File(_cleanDir);
			
			Util.massert(probe.exists() && probe.isDirectory(), 
				"Clean list directory %s not found, must create empty directory before starting.", _cleanDir);

			for(String oneday : getDateRange(SliUtil.CLEANLIST_SAVE_WINDOW))
			{
				// e.g. 2012-06-15____clean.txt
				String savepath = SliUtil.getCleanListPath(oneday);
				// Util.pf("Clean path is %s\n", savepath);
				if(!(new File(savepath)).exists())
				{
					Util.pf("WARNING: no cleanlist found for daycode %s\n", oneday);	
					continue;
				}
				
				List<String> flines = FileUtils.readFileLinesE(savepath);
				Util.setdefault(_cleanMap, oneday, new TreeSet<String>());
				_cleanMap.get(oneday).addAll(flines);
				Util.massert(_cleanMap.get(oneday).size() == flines.size(), "Duplicates found in clean list save path");
				
				Util.pf("Added %d files for daycode %s\n", _cleanMap.get(oneday).size(), oneday);
			}
		}
		
		public void reportFinished(Collection<String> compset)
		{
			for(String oneclean : compset)
				{ addToClean(oneclean);	}	
			
			dropExcess();
			persist();
		}
		
		private void dropExcess()
		{
			while(_cleanMap.size() > SliUtil.CLEANLIST_SAVE_WINDOW)
				{ _cleanMap.pollFirstEntry(); }
		}
		
		private void persist()
		{
			for(String daycode : _cleanMap.keySet())
			{
				String cleanpath = SliUtil.getCleanListPath(daycode);
				FileUtils.writeFileLinesE(_cleanMap.get(daycode), cleanpath);
				// Util.pf("Wrote %d files to path %s\n", _cleanMap.get(daycode).size(), cleanpath);
			}
			
			Util.pf("Wrote %d clean list files, last daycode is %s\n",
				_cleanMap.size(), _cleanMap.lastKey());
		}
		
		private void addToClean(String nowcleanpath)
		{
			Calendar nfscal = TimeUtil.calFromNfsPath(nowcleanpath);
			String daycode = TimeUtil.cal2DayCode(nfscal);	
			
			Util.setdefault(_cleanMap, daycode, new TreeSet<String>());
			_cleanMap.get(daycode).add(nowcleanpath);
		}
		
		private boolean pathIsClean(String checkpath)
		{
			Calendar nfscal = TimeUtil.calFromNfsPath(checkpath);
			String daycode = TimeUtil.cal2DayCode(nfscal);
			
			return _cleanMap.containsKey(daycode) && _cleanMap.get(daycode).contains(checkpath);
		}
		
		private List<String> getDateRange(int goback)
		{
			List<String> daylist = TimeUtil.getDateRange(goback);
			daylist.add(TimeUtil.getTodayCode());
			return daylist;
		}
		
		public List<String> nextBatch(int lookback)
		{
			List<String> newlist = Util.vector();
			for(String oneday : getDateRange(lookback))
			{
				for(String onepath : SliUtil.getPathsForDay(oneday))
				{
					if(!pathIsClean(onepath))
						{ newlist.add(onepath); }
				}
			}

			return newlist;
		}
		
		// Returns true if all the paths for the given day are clean.
		@Override		
		public boolean finishedForDay(String daycode)
		{
			// Never report that we have finished for Monday before 1AM on Tuesday.
			String validafter = Util.sprintf("%s 01:00:00", daycode);
			String tsnow = TimeUtil.longDayCodeNow();
			
			if(tsnow.compareTo(validafter) < 0)
				{ return false; }	

			for(String onepath : SliUtil.getPathsForDay(daycode))
			{
				if(!pathIsClean(onepath))
					{ return false; }
			}			
			
			Util.pf("finished4Day:: time now is %s, valid after is %s, returning true",
				tsnow, validafter);
			
			return true;
		}
		
		public void printStatusInfo()
		{
			Util.pf("Clean list has %d entries as follows\n", _cleanMap.size());
			
			for(String oneday : _cleanMap.keySet())
			{	
				Util.pf("\tFound %d clean files for %s\n", _cleanMap.get(oneday).size(), oneday);
				
			}
			Util.pf("Clean List has %d elements, last saved at %s",
				_cleanMap.size());
		}
	}
	
	public static class DbImpl extends ControlTable
	{
		Map<String, SortedMap<String, TableEntry>> lookupTable = Util.treemap();
		
		List<String> dayList = Util.vector();
		
		private final Set<String> _batchSet = Util.treeset();
		
		public DbImpl(String dc, int lookback)
		{
			dayList = TimeUtil.getDateRange(dc, lookback);
		}
		
		
		public static void main(String[] args)
		{
			/*
			ControlTable ctable = new ControlTable();
			ctable.loadFromDb();
			ctable.loadRecentPaths(1);
			
			ctable.saveToDb();
			
			*/
		}
		
		@Override
		public boolean finishedForDay(String daycode)
		{
			throw new RuntimeException("Not yet implemented for DbImpl");
		}		
		
		void loadFromDb()
		{
			/*
			int loadcount = 0;
			
			try { 
			Connection conn = SliDatabase.getConnection();
			PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM control_table WHERE fdate = ?");
			
			for(String oneday : dayList)
			{
			Util.setdefault(lookupTable, oneday, new TreeMap<String, TableEntry>());
			
			pstmt.setString(1, oneday);
			ResultSet rset = pstmt.executeQuery();
			while(rset.next())
			{
			String fpath = rset.getString("fpath");
			String fdate = rset.getString("fdate");
			String comp = rset.getString("completed");
			
			TableEntry tent = new TableEntry(fdate, comp);
			lookupTable.get(oneday).put(fpath, tent);
			loadcount++;
			}
			}
			
			conn.close();
			Util.pf("Loaded %d rows from control table\n", loadcount);
			
			} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);
			}
			*/
		}
		
		void persistTable()
		{
			try { 
				Connection conn = SliDatabase.getConnection();
				
				PreparedStatement dlstmt = conn.prepareStatement("DELETE FROM control_table WHERE fpath = ?");
				PreparedStatement instmt = conn.prepareStatement("INSERT INTO control_table (fpath, fdate, completed, started) VALUES (?, ?, ?, ?)");
				
				int upcount = 0;
				Util.pf("Updating control table. ");
				
				for(String oneday : lookupTable.keySet())
				{
					for(String onepath : lookupTable.get(oneday).keySet())
					{
						TableEntry tent = lookupTable.get(oneday).get(onepath);
						
						if(tent.isDirty())
						{
							Util.pf(".");
							
							dlstmt.setString(1, onepath);
							int delrows = dlstmt.executeUpdate();
							
							instmt.setString(1, onepath);
							instmt.setString(2, tent.dayCode);
							instmt.setString(3, tent.compTime);
							instmt.setString(4, tent.startedTime);
							int insrows = instmt.executeUpdate();
							
							// TODO: arguably this isn't the right place to do this, but...
							tent.markClean();
							upcount++;
						}
					}
				}
				conn.close();
				
				Util.pf(" done, updated %d table entries\n", upcount);
				
			} catch (SQLException sqlex) {
				
				throw new RuntimeException(sqlex);
			}
		}
		
		public List<String> nextBatch(int lookback)	
		{
			Vector<String> pathset = new Vector<String>(_batchSet);
			return pathset;
		}	
		
		Map<String, int[]> startCompleteMap()
		{
			Map<String, int[]> startmap = Util.treemap();
			
			for(String daycode : lookupTable.keySet())
			{
				Util.setdefault(startmap, daycode, new int[2]);
				
				for(TableEntry tent : lookupTable.get(daycode).values())
				{
					int relind = (tent.isComplete() ? 1 : 0);
					startmap.get(daycode)[relind] += 1;
				}
			}
			
			return startmap;
		}
		
		public void printStatusInfo()
		{
			Map<String, int[]> scmap = startCompleteMap();
			
			for(String daycode : scmap.keySet())
			{
				Util.pf("For daycode=%s, have %d started vs %d completed files\n", 
					daycode, scmap.get(daycode)[0], scmap.get(daycode)[1]);
			}
		}
		
		private void loadNewPaths(int numdays, int maxadd)
		{
			List<String> dayrange = TimeUtil.getDateRange(numdays);
			Set<String> pathset = Util.treeset();
			int newcount = 0;
			String startTime = Util.cal2LongDayCode(new GregorianCalendar());
			
			for(String oneday : dayrange)
			{ 
				Util.setdefault(lookupTable, oneday, new TreeMap<String, TableEntry>());
				SortedMap<String, TableEntry> relmap = lookupTable.get(oneday);
				
				for(String onepath : SliUtil.getPathsForDay(oneday))
				{
					if(!relmap.containsKey(onepath))
					{
						TableEntry tent = new TableEntry(oneday, startTime);
						relmap.put(onepath, tent);
						newcount++;
						
						if(newcount >= maxadd)
							{ return; }
					}
				}
			}
		}	
	
		public void reportFinished(Collection<String> pathset)
		{
			int fcount = 0;
			
			for(String daycode : lookupTable.keySet())
			{
				for(String onepath : lookupTable.get(daycode).keySet())
				{
					if(pathset.contains(onepath))
					{
						lookupTable.get(daycode).get(onepath).markComplete();	
						fcount++;
					}
				}
			}
			
			Util.massert(fcount == pathset.size(), "Error: some reported paths not found in lookupTable");
			
			// Save changes to database
			persistTable();
		}
		
	}

	
	static class TableEntry 
	{
		String dayCode;
		String compTime;
		String startedTime; 
		
		// Table Entries are actually set as dirty to BEGIN with, so they are not saved to begin with
		private boolean dirty = false;
		
		public TableEntry(String dc, String stime)
		{
			this(dc, stime, null);
		}
		
		private TableEntry(String dc, String stime, String ctime)
		{
			dayCode = dc;
			startedTime = stime;			
			compTime = ctime;
		}
		
		void markComplete()
		{
			Calendar cal = new GregorianCalendar();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			compTime = sdf.format(cal.getTime());
			dirty = true;
		}
		
		void markStarted()
		{
			Calendar cal = new GregorianCalendar();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			startedTime = sdf.format(cal.getTime());
			dirty = true;			
		}
		
		void markClean()
		{
			dirty = false;	
		}
		
		boolean isDirty()
		{
			return dirty;
		}
		
		boolean isComplete()
		{
			return (compTime != null);
		}
	}

}
