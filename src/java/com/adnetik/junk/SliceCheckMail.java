
package com.adnetik.userindex;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;

// Going to focus first on generating SINGLE reports from automatically generated data.
public class SliceCheckMail extends Configured implements Tool
{	
	private SortedSet<String> _prevWeek = Util.treeset();
	private SortedSet<String> _currWeek = Util.treeset();
	
	private Map<String, Map<String, Long>> _checkMap = Util.treemap();
	
	private SimpleMail _logMail;
		
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{
		setWeekSetInfo(TimeUtil.getTodayCode());
		
		List<String> probelist = getProbeList();
		
		List<String> newlist = Util.vector();
		List<String> prvlist = Util.vector();
		
		for(String listcode : ListInfoManager.getSing().getFullListCodeSet())
		{
			Map<String, Long> sizemap = getSliceSizeMap(listcode, probelist);
			(isPrev(sizemap) ? prvlist : newlist).add(listcode);
			
			_checkMap.put(listcode, sizemap);
		}
		
		{
			List<String> headlist = Util.vector();
			headlist.add("LISTCODE");
			headlist.add("NICKNAME");
			headlist.add("");
			headlist.addAll(_currWeek);
			_logMail.pf("%s\n", Util.join(headlist, "\t"));
		}
		
		_logMail.pf("NEWLY ENTERED CODES\n");
		printListInfo(newlist);
		
		// for(String anew : newlist)
		//	{ _logMail.pf("%s\n", printSliceLine(anew)); }

		_logMail.pf("PREVIOUSLY ENTERED CODES\n");
		
		// for(String aprv : prvlist)
		//	{  _logMail.pf("%s\n", printSliceLine(aprv)); }
		
		
		
		printListInfo(prvlist);
		
		/*
		List<String> daylist = TimeUtil.getDateRange(10);
		Map<String, Map<String, String>> dateCodeSize = Util.treemap();
		
		SimpleMail logMail = new SimpleMail(Util.sprintf("User Index Slice Data %s", TimeUtil.getTodayCode()), false);
		
		for(String oneday : daylist)
			{ setSizeInfo(dateCodeSize, oneday); }
		
		for(String listcode : dateCodeSize.keySet())
		{
			List<String> infolist = Util.vector();
			for(String datekey : dateCodeSize.get(listcode).keySet())
			{
				infolist.add(Util.sprintf("%s=%s", datekey, dateCodeSize.get(listcode).get(datekey)));	
			}
			
			logMail.pf("\nList code %s :: \t\t%s", listcode, Util.join(infolist, "\t"));
		} 
		*/
		
		// _logMail.send2admin();

		// Ad Ops Group - US
		// logMail.send("usadops@digilant.com");	
		
		// Nate
		// logMail.send("nathan.woodman@digilant.com");
		
		return -1;
	}
	
	private void printListInfo(List<String> listlist)
	{
		SortedSet<Pair<String, String>> listset = Util.treeset();
		
		for(String onelist : listlist)
		{
			String nick = ListInfoManager.getSing().getNickName(onelist);
			listset.add(Pair.build(nick, onelist));
		}
		
		for(Pair<String, String> onepair : listset)
		{
			_logMail.pf("%s\n", printSliceLine(onepair._2));
		}
	}
	
	private String printSliceLine(String listcode)
	{
		Util.massert(_checkMap.containsKey(listcode));
		
		List<String> rowlist = Util.vector();
		rowlist.add(listcode);
		rowlist.add(ListInfoManager.getSing().getNickName(listcode));
		rowlist.add("");
		
		for(String curday : _currWeek)
		{ 
			Long toprint = _checkMap.get(listcode).get(curday);
			rowlist.add((toprint == null ? "NO DATA" : humanReadLen(toprint)));
		}
		return Util.join(rowlist, "\t");
		
	}
	
	boolean isPrev(Map<String, Long> sizemap)
	{
		return sizemap.keySet().containsAll(_prevWeek);
	}
	
	List<String> getProbeList()
	{
		List<String> plist = Util.vector();
		plist.addAll(_prevWeek);
		plist.addAll(_currWeek);
		return plist;
	}
	
	void setWeekSetInfo(String targday)
	{
		_logMail = new SimpleMail("User Index Slice Check Mail " + targday, false);
		
		String probeday = targday;
		
		while(true)
		{
			probeday = TimeUtil.dayBefore(probeday);
			_currWeek.add(probeday);

			if(UserIndexUtil.isBlockStartDay(probeday))
				{ break; }
		}
		
		while(true)
		{
			probeday = TimeUtil.dayBefore(probeday);
			_prevWeek.add(probeday);

			if(UserIndexUtil.isBlockStartDay(probeday))
				{ break; }
		}		
		
		Util.massert(_prevWeek.size() == 7 && _currWeek.size() <= 7, "Error in date calculations");
	}
	
	
	Map<String, Long> getSliceSizeMap(String listcode, Collection<String> daylist) throws IOException
	{
		Map<String, Long> sizemap = Util.treemap();
		FileSystem fsys = FileSystem.get(getConf());
		
		for(String oneday : daylist) 
		{
			Path slicepath = new Path(Util.sprintf("/userindex/dbslice/%s/%s.%s", oneday, listcode, UserIndexUtil.SLICE_SUFF));

			if(fsys.exists(slicepath))
				{ sizemap.put(oneday, fsys.getFileStatus(slicepath).getLen()); }
		}
		return sizemap;
	}
	
	void setSizeInfo(Map<String, Map<String, String>> dateCodeSize, String daycode) throws IOException
	{
		FileSystem fsys = FileSystem.get(getConf());
		String pathpatt = Util.sprintf("/userindex/dbslice/%s/*.%s", daycode, UserIndexUtil.SLICE_SUFF);
		List<Path> pathlist = HadoopUtil.getGlobPathList(getConf(), pathpatt);
		
		for(Path p : pathlist)
		{
			String listcode = p.getName();
			listcode = listcode.substring(0, listcode.length()-("." + UserIndexUtil.SLICE_SUFF).length());
			
			Long pathlen = fsys.getFileStatus(p).getLen();
			
			Util.setdefault(dateCodeSize, listcode, new TreeMap<String, String>());
			
			dateCodeSize.get(listcode).put(daycode.substring(5), humanReadLen(pathlen)); 
		}
	}
	
	String humanReadLen(long flen)
	{
		String suff = "B";
		
		if(flen > 1000)	
		{
			flen /= 1000;
			suff = "KB";
		}
		
		if(flen > 1000)
		{
			flen /= 1000;
			suff = "MB";			
		}
		
		if(flen > 1000)
		{
			flen /= 1000;	
			suff = "GB";
		}
		
		return Util.sprintf("%d%s", flen, suff);
		
	}
}
