
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import java.text.SimpleDateFormat;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.ExcName;

import com.adnetik.shared.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.analytics.HistogramTool;

/** 
 *  Statistics about the slice data accumulated in the scanning process.
 */ 
public class SliceStats
{	
	List<String> _dayList;
	String _canDay;
	
	Map<String, Integer> _searchCountMap = Util.treemap();
	Map<String, ListPackage> _listPackMap = Util.treemap();
	
	FileSystem _fSystem;
	
	List<Integer> _calloutBuckList = Arrays.asList(new Integer[] { 0, 1, 2, 3, 4, 5, 8, 12, 20, 50, 100, 200, 300, 400, 500, 1000, 2000, Integer.MAX_VALUE });
	
	
	public static void main(String[] args) throws Exception
	{
		SliceStats scomp = new SliceStats();
		scomp.compileSearchCountMap();
		// scomp.grabListData(2);		
		scomp.grabListData();
		
		List<String> csvlist = scomp.getCsvLines();
		
		for(String onecsv : csvlist)
		{
			Util.pf("%s\n", onecsv);	
			
		}
		
		String statpath = Util.sprintf("%s/stats/slicestats_%s.csv", 
			UserIndexUtil.LOCAL_UINDEX_DIR, scomp._canDay);
		FileUtils.writeFileLinesE(csvlist, statpath);
	}
	
	public SliceStats() throws IOException
	{
		this(UserIndexUtil.getCanonicalEndDaycode());
	}
		
	public SliceStats(String daycode) throws IOException
	{
		Util.massert(UserIndexUtil.isBlockEndDay(daycode), "Must call with a canonical end day");
		_canDay = daycode;	
		_dayList = UserIndexUtil.getCanonicalDayList(daycode);
		
		_fSystem = FileSystem.get(new Configuration());
	}
	
	// TODO: redo with new STagingInfo setup
	void compileSearchCountMap() throws IOException
	{
		Util.massert(false, "Need to reimplement");
		/*
		String blockstart = UserIndexUtil.getBlockStartForDay(_canDay);
		
		for(UserIndexUtil.StagingType stype : UserIndexUtil.StagingType.values())
		{
			Path stagepath = new Path(UserIndexUtil.getStagingInfoPath(stype, blockstart));
		
			if(!_fSystem.exists(stagepath))
			{
				Util.pf("Found no staging info for %s, %s", stype, blockstart);	
				continue;
			}
			
			BufferedReader bread = HadoopUtil.hdfsBufReader(_fSystem, stagepath);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				String listcode = oneline.split("\t")[1];
				Util.incHitMap(_searchCountMap, listcode);
			}
			bread.close();
		}
		
		for(String listcode : _searchCountMap.keySet())
		{
			Util.pf("Found %d users for listcode %s\n", _searchCountMap.get(listcode), listcode);				
		}		
		*/
	}
	
	List<String> getCsvLines()
	{
		List<String> csvlines = Util.vector();

		List<String> headlist = Util.vector();	
		{
			
			headlist.add("listcode");
			headlist.add("nickname");
			
			headlist.add("SEARCH users");
			headlist.add("FOUND users");
			headlist.add("# callouts");
			headlist.add("avg #call/user");
			headlist.add("median #call/user");
			
			for(int i = 1; i <= 7; i++)
				{ headlist.add(i + " days"); }
			
			for(int callbuck : _calloutBuckList)
			{
				if(callbuck == 0) 
					{ continue; }
				
				headlist.add(Util.sprintf(" :%d callouts", callbuck));
			}
			
			csvlines.add(Util.join(headlist, ","));
		}
		
		for(String listcode : _listPackMap.keySet())
		{
			ListPackage lpack = _listPackMap.get(listcode);
			
			List<String> clist = Util.vector();
			clist.add(listcode);
			clist.add(ListInfoManager.getSing().getNickName(listcode));
			
			clist.add("" + _searchCountMap.get(listcode));
			clist.add("" + lpack.numUsers());
			clist.add("" + lpack.numCallouts());
			clist.add(Util.sprintf("%.03f",  ((double) lpack.numCallouts())/lpack.numUsers()));
			clist.add("" + lpack.getMedian());
			
			int[] daycount = lpack.usersForDayCount();
			Util.massert(daycount[0] == 0);

			for(int i = 1; i <= 7; i++)
			{
				clist.add("" + daycount[i]);
			}
			
			for(Pair<Number, Double> onepair : lpack._hTool.getBinWeightList())
			{
				if(onepair._1.intValue() < 1)
					{ continue; }
				
				clist.add(""+Math.round(onepair._2));
			}
			
			Util.massert(headlist.size() == clist.size(), "Mismatch between header and data row");
			
			csvlines.add(Util.join(clist, ","));
		}
		
		return csvlines;
	}
	
	void grabListData() throws IOException
	{
		grabListData(100000);
	}	
	
	void grabListData(int max) throws IOException
	{
		List<String> listlist = new Vector<String>(_searchCountMap.keySet());
		
		for(int i = 0; i < listlist.size() && i < max; i++)
		{ 
			runForList(listlist.get(i));
		}
	}	
	
	void runForList(String listcode) throws IOException
	{
		ListPackage lpack = new ListPackage(listcode);	
		lpack.compileData();
		
		_listPackMap.put(listcode, lpack);
	}
	
	private class ListPackage
	{
		String _listCode;
		
		// ID --> number of callouts for user		
		Map<WtpId, Integer> _idCountMap = Util.treemap();

		// ID --> set of unique days for which user was seen
		Map<WtpId, Set<String>> _sawUserMap = Util.treemap();
		
		HistogramTool<Integer> _hTool;
		
		public ListPackage(String lc)
		{
			_listCode = lc;			
			_hTool = new HistogramTool<Integer>(_calloutBuckList);
		}
		
		void compileData() throws IOException
		{		
			double startup = Util.curtime();
			for(String oneday : _dayList)
			{
				String slicepath = Util.sprintf("/userindex/dbslice/%s/%s.%s", oneday, _listCode, UserIndexUtil.SLICE_SUFF);
				// String slicepath = UserIndexUtil.getHdfsSlicePath(oneday, _listCode);
				compileForPath(oneday, slicepath);
			}
			
			Util.pf("Finished compiling for listcode=%s, took %.03f, %d total callouts\n",
				_listCode, (Util.curtime()-startup)/1000, numCallouts());
			
			for(Integer numcall  : _idCountMap.values())
			{
				_hTool.incrementValue(numcall);		
			}
			
			// Util.pf("Total mass is %.03f, count size is %d\n", _hTool.getTotalMass(), _idCountMap.size());
			Util.massert(Math.abs(_hTool.getTotalMass()-_idCountMap.size()) < 1e-6);			
			
		}	
		
		void compileForPath(String daycode, String testpath) throws IOException
		{
			BufferedReader bread = HadoopUtil.getGzipReader(_fSystem, testpath);
			compileForReader(daycode, bread);
			bread.close();
			
			// Util.pf("Finished reading for testpath %s\n", testpath);
		}
		
		void compileForReader(String daycode, BufferedReader bread) throws IOException
		{
			Set<String> idset = Util.treeset();
			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				String[] toks = oneline.split("\t");
				WtpId wid = new WtpId(toks[0].split(Util.DUMB_SEP)[1]);
				
				Util.incHitMap(_idCountMap, wid);
							
				Util.setdefault(_sawUserMap, wid, new TreeSet<String>());
				_sawUserMap.get(wid).add(daycode);
			}
		}		
		
		public int[] usersForDayCount()
		{
			int[] daycount = new int[8];
			
			for(Set<String> sepset : _sawUserMap.values())
			{
				int m = sepset.size();
				daycount[m] += 1;
			}
			
			return daycount;
		}
		
		public int numUsers()
		{
			return _idCountMap.size();
		}
		
		public int numCallouts()
		{
			int nc = 0; 
			
			for(int c : _idCountMap.values())
				{ nc += c; }
			
			return nc;
		}
		
		public int getMedian()
		{
			return Util.getMedian(Util.metaCountMap(_idCountMap));
		}		
		
	}
	
}
