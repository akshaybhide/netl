
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;

public class CheckReady extends Configured implements Tool
{		
	private String _blockEnd;
	
	FileSystem fSystem;
	
	TreeMap<String, TreeSet<String>> _sliceUseMap = Util.treemap();
	
	List<String> probeList; 
	
	//public static final int PART_FILE_SIZE_CUTOFF = 6296401766	
	// public static final long PART_FILE_SIZE_CUTOFF_GZP = 1000000000L;
	
	public static final long PART_FILE_SIZE_CUTOFF_GZP = 1000000000L;
	// public static final long PART_FILE_SIZE_CUTOFF_TXT = 5000000000L;	
	
	// Say 1MB is the cutoff for slice files
	// public static final long SLICE_PATH_CUTOFF_TXT = 1000000L;
	
	public static final long SLICE_PATH_CUTOFF_GZP = 10000L;
	
	public static void main(String[] args) throws Exception
	{
		int mainCode = HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{	
		
		ArgMap argmap = Util.getClArgMap(args);
		String blockend = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
		blockend = argmap.getString("blockend", blockend);
		
		setDayCode(blockend);
		
		runCheck();
		
		buildReadyMap();
		
		return 1;
	}
	
	public void setDayCode(String dc) throws IOException
	{
		_blockEnd = dc;
		probeList = UserIndexUtil.getCanonicalDayList(_blockEnd);
			
		fSystem = FileSystem.get(new Configuration());
		
		for(String oneday : probeList)
			{ buildSliceUsageMap(oneday); }		
	}
	
	void runCheck() throws IOException
	{
		Util.pf("Checking User Index data for:\n\tStart:%s\n\tEnd  :%s\n",
			probeList.get(0), probeList.get(probeList.size()-1));
		
		showRelDates();
		showSliceInfo();		
	}
	
	void showRelDates() throws IOException
	{
		for(String probe : probeList)
		{
			checkPartFilesReady(probe);
		}
	}
	
	public boolean isListReady(String listcode)
	{
		Util.massert(_sliceUseMap != null, "Must initialize by calling setDayCode first");
		
		// Have as many days of slice data as we checked.
		return _sliceUseMap.containsKey(listcode) && _sliceUseMap.get(listcode).size() == probeList.size();
	}
	
	void showSliceInfo() throws IOException
	{
		
		SortedSet<Pair<String, String>> goodset = Util.treeset();
		
		for(String listcode : _sliceUseMap.keySet())
		{
			if(isListReady(listcode))
			{
				String nickname = ListInfoManager.getSing().getNickName(listcode);
				goodset.add(Pair.build(nickname, listcode));
			}
			else if(_sliceUseMap.get(listcode).size() > 0) 
				{ Util.pf("Found %d slice day info for %s\n", _sliceUseMap.get(listcode).size(), listcode); }
		}
		
		Util.pf("Found %d full slice paths:\n", goodset.size());		
		
		for(Pair<String, String> onepair : goodset)
		{
			Util.pf("\t%s\t%s\n", Util.padstr(onepair._1, 20), Util.padstr(onepair._2, 20));
		}
		
	}
	
	void buildReadyMap() throws IOException
	{
		// Just use for 
		List<String> datalist = Util.vector();
		
		for(String listcode : _sliceUseMap.keySet())
		{
			if(isListReady(listcode))
			{
				int lcid = datalist.size();
				String record = Util.sprintf("%s\t%d", listcode, lcid);
				datalist.add(record);
			}
		}		
		
		HadoopUtil.writeLinesToPath(datalist, fSystem, UserIndexUtil.getReadyMapPath(_blockEnd));
		Util.pf("Wrote %d READY MAP lines\n", datalist.size());
	}
	
	static Map<String, Integer> loadReadyMap(String blockend)
	{
		List<String> lclist = HadoopUtil.readFileLinesE(UserIndexUtil.getReadyMapPath(blockend));
		Map<String, Integer> readymap = Util.treemap();
		
		for(String onelc : lclist)
		{
			String[] toks = onelc.split("\t");
			readymap.put(toks[0], Integer.valueOf(toks[1]));
		}
		
		return readymap;
	}
	
	void checkPartFilesReady(String oneday) throws IOException
	{
		boolean dayokay = true;
		
		for(int i = 0; i < 24; i++)
		{
			dayokay &= partFileOkay(oneday, i);
		}
		
		if(dayokay)
			{ Util.pf("Part files are okay for %s\n", oneday); }
		else
			{ Util.pf("Problem with part files for %s\n", oneday); }
	}
	
	boolean partFileOkay(String oneday, int partind) throws IOException
	{
		
		String partpatt = Util.sprintf("/userindex/sortscrub/%s/part-%s.*", 
			oneday, Util.padLeadingZeros(partind, 5));
		
		List<Path> matchlist = HadoopUtil.getGlobPathList(fSystem, partpatt);
		
		if(matchlist.size() != 1)
		{
			//Util.pf("Missing file corresponding to %s\n", partpatt);	
			return false;
		}
		
		long relcutoff = PART_FILE_SIZE_CUTOFF_GZP;
		
		Util.massert(matchlist.get(0).toString().endsWith(".gz"),
			"Found non-gzipped partfile %s", matchlist.get(0));
				
		Long flen = fSystem.getFileStatus(matchlist.get(0)).getLen();
		
		if(flen < relcutoff)
		{
			Util.pf("Part file %d is too small for %s\n", flen, partpatt);
			return false;
		}
		
		// Util.pf("File found for daycode=%s, partind=%d\n", oneday, partind);
		return true;
	}
	
	void buildSliceUsageMap(String oneday) throws IOException
	{
		buildSliceUseMap(oneday, fSystem, _sliceUseMap);
	}
	
	static void buildSliceUseMap(String oneday, FileSystem fsys, Map<String, TreeSet<String>> sliceusemap) throws IOException
	{		
		String slicepatt = Util.sprintf("/userindex/dbslice/%s/*.%s", oneday, UserIndexUtil.SLICE_SUFF);
		List<Path> matchlist = HadoopUtil.getGlobPathList(fsys, slicepatt);
		
		for(Path p : matchlist)
		{
			long relcutoff = SLICE_PATH_CUTOFF_GZP;
			
			Util.massert(p.toString().endsWith(".gz"), "Found non-gzip slice file %s", p);
			
			if(fsys.getFileStatus(p).getLen() <  relcutoff)
				{ continue; }
			
			String listcode = p.getName();
			listcode = listcode.substring(0, listcode.length()-UserIndexUtil.SLICE_SUFF.length()-1);

			Util.setdefault(sliceusemap, listcode, new TreeSet<String>());
			sliceusemap.get(listcode).add(oneday);			
		}				
	}
	
	static List<String> getReadyListenList(FileSystem fsys, String blockend) throws IOException
	{
		Map<String, TreeSet<String>> slimap = Util.treemap();
		List<String> candaylist = UserIndexUtil.getCanonicalDayList(blockend);
		
		for(String oneday : candaylist)
			{ buildSliceUseMap(oneday, fsys, slimap); }
		
		// Oneliner in Python, I hate my life
		List<String> mylist = Util.vector();
		for(String listcode : slimap.keySet())
		{
			if(slimap.get(listcode).size() == candaylist.size())
				{ mylist.add(listcode); }
		}
		return mylist;		
	}
	
}
