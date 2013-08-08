
package com.adnetik.bm_etl;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;


public class HdShard2Db extends Configured implements Tool
{
	SimpleMail _logMail = new SimpleMail("Domain Shard2Db");
	String dayCode;
	Set<Integer> completedSet = Util.treeset();
	
	boolean isInternal;
	Map<Integer, Integer> infLineCount = Util.treemap();
	
	Set<Integer> campIdSet = Util.treeset();
	
	SortedSet<DimCode> dimSet;
	SortedSet<IntFact> intSet;
	SortedSet<DblFact> dblSet;
	
	private static final String SPOOL_FILE = BmUtil.LOCAL_UTIL_DIR + "/hd_shard2db_spool.txt";
	private static final String IN_FILE = BmUtil.LOCAL_UTIL_DIR + "/hd_shard2db_inf.txt";
	
	public static int DOMAIN_CUTOFF = 500;
	
	PrintWriter spoolWriter;
	
	Map<String, Integer> domCountMap = Util.treemap();
	int curCamp = -1;
	
	Connection shareConn;
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);	
	}
	
	
	public int run(String[] args) throws Exception
	{		
		// TODO: What the fuck is this?
		Class.forName("com.adnetik.bm_etl.View2Hard");
		
		if(args.length < 1 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: HdShard2Db <daycode>\n");	
			return 1;			
		}
		
		Map<String, String> optargs = Util.getClArgMap(args);
		boolean deleteold = !("false".equals(optargs.get("deleteold")));
		
		dayCode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		isInternal = false;
		
		shareConn = DatabaseBridge.getDbConnection(DbTarget.external);
		dimSet = DatabaseBridge.getDimSet(AggType.ad_domain, DbTarget.external);
		dblSet = DatabaseBridge.getDblFactSet(AggType.ad_domain, DbTarget.external);
		intSet = DatabaseBridge.getIntFactSet(AggType.ad_domain, DbTarget.external);

		// Delete old data
		if(deleteold) 
		{
			_logMail.pf("Deleting old data for %s... ", dayCode);
			int delrows = DatabaseBridge.deleteOld(AggType.ad_domain, dayCode, DbTarget.external);
			_logMail.pf(" ... done, deleted %d rows \n", delrows);
		}
		
		transformResult(FileSystem.get(getConf()));
				
		shareConn.close();
		
		View2Hard.diffUpdate(campIdSet, dayCode, false, _logMail);
		_logMail.send2admin();
		
		return 1;
	}	
	
	public void transformResult(FileSystem fsys) throws Exception
	{
		String respatt = BmUtil.getOutputPath(dayCode, DbTarget.external) + "part-*";
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, respatt);	
			
		_logMail.pf("\nFound %d paths for pattern %s", pathlist.size(), respatt);
		
		for(Path p : pathlist)
		{
			BufferedReader bufread = HadoopUtil.hdfsBufReader(fsys, p);
			spool(bufread);
			bufread.close();
			_logMail.pf("Finished processing path %s\n", p);
		}
		
	}	
	
	void flush2db() throws IOException,SQLException
	{
		// Each campaign is only going to be flushed once
		{
			Util.massert(!completedSet.contains(curCamp), "Campaign %d already processed", curCamp);
			completedSet.add(curCamp);
		}
	
		Set<String> topset = getTopDomains();
		domCountMap.clear();
		
		//Util.pf("\nFound %d top domains for campaign %s, hitmap is %d", topset.size(), curCamp, domHitMap.size());
		
		{
			Map<HackKey, Metrics> othermap = Util.treemap();
			PrintWriter infwrite = new PrintWriter(IN_FILE);
			BufferedReader bread = Util.getReader(SPOOL_FILE);
			
			for(String spline = bread.readLine(); spline != null; spline = bread.readLine())
			{
				Pair<HackKey, Metrics> mpair = hdParseLine(spline);
				String d = mpair._1.getField(DimCode.domain.toString());
				
				if(topset.contains(d))
					{ writeInfLine(mpair._1, mpair._2, infwrite); }
				else
				{
					//Util.pf("\nDomain is %s, not in topset", d);
					mpair._1.setField(DimCode.domain.toString(), "OTHERS");	
					if(!othermap.containsKey(mpair._1))
						{ othermap.put(mpair._1, new Metrics()); }
					othermap.get(mpair._1).add(mpair._2);
				}
			}
			bread.close();
			
			// Write the others
			for(HackKey hkey : othermap.keySet())
				{ writeInfLine(hkey, othermap.get(hkey), infwrite); }
			
			infwrite.close();
			// Util.pf("\nFinished writing to infile, got %d hits for campaign %d", infLineCount.get(curCamp), curCamp);
			
			// And we're off...
			DbUtil.loadFromFile(new File(IN_FILE), "ad_domain", getColNames(), shareConn);
			
		}
	}
	
	void spool(BufferedReader bufread) throws Exception
	{
		for(String line = bufread.readLine(); line != null; line = bufread.readLine())
		{
			String[] key_val = line.split("\t");
			Map<String, String> parsemap = BmUtil.getParseMap(key_val[0]);
			
			if("NOTSET".equals(parsemap.get("campaign")))
				{ continue; }
			
			// Check that if aggtype is present, it is "ad_domain".
			String aggtype = parsemap.get("aggtype");
			if(aggtype != null && !aggtype.equals(AggType.ad_domain.toString()))
				{ continue; }
			
			int newcamp = Integer.valueOf(parsemap.get(DimCode.campaign.toString()));
			if(newcamp != curCamp)
			{		
				if(curCamp != -1)
				{
					// Stop writing to spool file, since now we're going to write to it.
					spoolWriter.close();	
					spoolWriter = null; // Does this even make sense?				
					
					flush2db();
					
					_logMail.pf("Finished campaign %d, uploaded %d lines\n", curCamp, infLineCount.get(curCamp));
										
				}
				curCamp = newcamp;
				
				campIdSet.add(curCamp);
			}

			// Reinitialize spoolWriter if necessary			
			spoolWriter = (spoolWriter == null ? new PrintWriter(SPOOL_FILE) : spoolWriter);
			{
				String domain = parsemap.get(DimCode.domain.toString());
				Integer impcount = Integer.valueOf(BmUtil.getParseMap(key_val[1]).get(IntFact.impressions.toString()));
				// Big gotcha, counting number of lines instead of number of impressions
				// Util.incHitMap(domCountMap, domain, 1);
				Util.incHitMap(domCountMap, domain, impcount);
			}
			
			spoolWriter.write(line + "\n");
		}
	}
	
	private void writeInfLine(HackKey hkey, Metrics magg, PrintWriter infwrite)
	{
		infwrite.write(pair2inf(hkey, magg)+"\n");
		Util.incHitMap(infLineCount, curCamp, 1);
	}
	
	Pair<HackKey, Metrics> hdParseLine(String logline)
	{
		String[] key_val = logline.split("\t");
		HackKey hkey = HackKey.fromQueryStr(key_val[0]);
		Metrics magg = Metrics.fromQueryStr(key_val[1]);
		return Pair.build(hkey, magg);		
	}
	
	Set<String> getTopDomains()
	{
		SortedSet<Pair<Integer, String>> mset = new TreeSet<Pair<Integer, String>>(Collections.reverseOrder());
		
		for(String dom : domCountMap.keySet())
			{ mset.add(Pair.build(domCountMap.get(dom), dom)); }
		
		Set<String> topset = Util.treeset();
		
		for(Pair<Integer, String> toppair : mset)
		{ 
			topset.add(toppair._2); 
		
			if(topset.size() >= DOMAIN_CUTOFF)
				{ break; }
		}

		return topset;
	}
	
	String pair2inf(HackKey hkey, Metrics magg)
	{
		List<String> vlist = Util.vector();
		
		for(DimCode onedim : dimSet)
			{ vlist.add(hkey.getField(onedim.toString())); }
		
		for(IntFact ifact : intSet)
			{ vlist.add(magg.getField(ifact)+""); }
		
		for(DblFact dfact : dblSet)
			{ vlist.add(magg.getField(dfact)+""); }
		
		// Entry date
		vlist.add(dayCode);
			
		return Util.join(vlist, "\t");	
	}
	
	List<String> getColNames()
	{
		List<String> vlist = Util.vector();		
		
		for(DimCode onedim : dimSet)
		{ 
			if(onedim == DimCode.domain)
				{ vlist.add("NAME_DOMAIN"); }
			else
				{ vlist.add("ID_" + onedim.toString().toUpperCase()); }
		}
		
		for(IntFact ifact : intSet)
			{ vlist.add("NUM_" + ifact.toString().toUpperCase()); }
		
		for(DblFact dfact : dblSet)
			{ vlist.add("IMP_" + dfact.toString().toUpperCase()); } 

		vlist.add("ENTRY_DATE");
		
		return vlist;		
	}
	
	
	static class HackKey implements Comparable<HackKey>
	{
		private LinkedHashMap<String, String> parsemap = new LinkedHashMap<String, String>();
		
		public String toString()
		{
			return Util.join(parsemap.values(), "\t");
		}
		
		public static HackKey fromQueryStr(String qs)
		{
			HackKey hkey = new HackKey();
			Map<String, String> pmap = BmUtil.getParseMap(qs);
			hkey.parsemap.putAll(pmap);
			return hkey;
		}
		
		public void setDomain(String dom)
		{
			parsemap.put("NAME_DOMAIN", dom);	
		}
		
		
		public void setField(String fname, String newval)
		{
			parsemap.put(fname, newval);			
		}
		
		public String getField(String fname)
		{ 
			return parsemap.get(fname); 
		}
		
		public int compareTo(HackKey other)
		{
			return toString().compareTo(other.toString());	
		}
		
		public boolean equals(Object o)
		{
			if(o == null) 
				{ return false; }
			
			HackKey that = Util.cast(o);
			return toString().equals(that.toString());
		}
	}	
		
	
}
