
package com.adnetik.bm_etl;

import java.util.*;
import java.io.*;
import java.sql.*;

import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;


public class Shard2Db
{
	Metrics totalFact = new Metrics();
	Metrics checkFact = new Metrics();
	
	PrintWriter infile;
	int lcount = 0;
	int outputLines = 0;
	static int DOMAIN_LIMIT = 400;
	
	static String TEMP_INFILE = "temp_infile.txt";
	
	LinkedHashMap<String, Integer> topDomMap = new LinkedHashMap<String, Integer>();
	
	SortedMap<HackKey, Metrics> otherMap = Util.treemap();
	
	int campId; 
	String dayCode;
	
	public static void main(String[] args) throws Exception
	{
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);		

		deleteOld(daycode);
		
		//for(int cprobe = 930; cprobe < 931; cprobe++)		
		for(int cprobe = 0; cprobe < 10000; cprobe++)
		{
			Shard2Db s2db = new Shard2Db(cprobe, daycode);
			
			File shardfile = new File(s2db.getCampShardPath());
			if(!shardfile.exists())
				{ continue; }
			
			Util.pf("Found a shard file for campid=%d\n", cprobe);
			s2db.countImps();
			
			//FileUtils.serialize(dparse.topDomMap, "topdommap.ser");
			s2db.doAgg();
			s2db.doUpload();
			
			// break;
		}
	}
	
	Shard2Db(int cid, String dayc) throws IOException
	{
		infile = new PrintWriter("temp_infile.txt");
		
		campId = cid;
		dayCode = dayc;
	}
	
	void doAgg() throws IOException
	{
		double startup = Util.curtime();
		// topDomMap = FileUtils.unserializeEat("topdommap.ser");
		
		BufferedReader bread = sortNRead(getCampShardPath());
		//BufferedReader bread = Util.getReader("sort_camp_357.txt");
		
		HackKey curkey = null;
		Metrics curagg = null;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			Pair<HackKey, Metrics> myp = parseLine(oneline);
			HackKey newkey = myp._1;
			
			//if(curkey != null)
			// 	{ Util.pf("\nCur-New key is \n\t%s\n\t%s", curkey.toString(), newkey.toString()); }
			// Util.massert(curkey == null || curkey.toString().compareTo(newkey.toString()) <= 0);
			
			if(!newkey.equals(curkey))
			{
				send(curkey, curagg);
				
				curkey = newkey;
				curagg = new Metrics();
			}
			
			curagg.add(myp._2);
		}
		bread.close();
		
		send(curkey, curagg);
		
		sendOthers();
		
		infile.close();
		
		checkTotals();
		Util.pf("Wrote %d output lines to infile, took %.03f\n", outputLines, (Util.curtime()-startup)/1000);

	}
	
	static void deleteOld(String daycode)
	{
		String sql = Util.sprintf("DELETE FROM ad_domain WHERE entry_date = date('%s')", daycode);
		int delrows = DatabaseBridge.execSqlUpdate(sql, DbTarget.external);
		Util.pf("\nDeleted %d old rows for entry date %s", delrows, daycode);
	}
	
	static Pair<HackKey, Metrics> parseLine(String logline)
	{
		String[] key_val = logline.split("\t");
		
		HackKey hkey = HackKey.fromCommaStr(key_val[0]);

		Metrics magg = new Metrics();
		{
			int t = 0;
			String[] valtoks = key_val[1].split(",");
			
			for(IntFact ifact : IntFact.values())
				{ magg.setField(ifact, Integer.valueOf(valtoks[t++])); }
			
			for(DblFact dfact : DblFact.values())
				{ magg.setField(dfact, Double.valueOf(valtoks[t++])); }
		}
		return Pair.build(hkey, magg);
	}
	
	static class HackKey implements Comparable<HackKey>
	{
		private LinkedHashMap<String, String> parsemap = new LinkedHashMap<String, String>();
		
		public String toString()
		{
			return Util.join(parsemap.values(), "\t");
		}
		
		public static HackKey fromCommaStr(String c)
		{
			HackKey hkey = new HackKey();
			List<String> colnames = getDimColNames();
			String[] dimtoks = c.split(",");
			for(int i = 0; i < dimtoks.length; i++)
				{ hkey.parsemap.put(colnames.get(i), dimtoks[i]); }
			return hkey;
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
	
	
	void countImps() throws Exception
	{
		double startup = Util.curtime();
		Map<String, Integer> hitmap = Util.treemap();

		BufferedReader bread = Util.getReader(Log2Shard.getCampShardPath(campId, dayCode));
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			Pair<HackKey, Metrics> mypair = parseLine(oneline);
			String domain = mypair._1.getField("NAME_DOMAIN");
			int numimps = mypair._2.getField(IntFact.impressions);

			Util.setdefault(hitmap, domain, 0);
			Util.incHitMap(hitmap, domain, numimps);
			totalFact.add(mypair._2);			
		}
		bread.close();
		
		SortedSet<Pair<Integer, String>> sortlist = new TreeSet<Pair<Integer, String>>(Collections.reverseOrder());
		
		for(String dom : hitmap.keySet())
			{ sortlist.add(Pair.build(hitmap.get(dom), dom)); }

		for(Pair<Integer, String> top : sortlist)
		{
			topDomMap.put(top._2, top._1);
			
			if(topDomMap.size() >= DOMAIN_LIMIT)
				{ break; }			
		}
				
		Util.pf("Finished imp count found %d total, took %.03f\n", totalFact.getField(IntFact.impressions), (Util.curtime()-startup)/1000);
	}

	void send(HackKey hkey, Metrics magg) throws IOException
	{
		if(hkey == null) 
			{ return; }
		
		if(!topDomMap.containsKey(hkey.getField("NAME_DOMAIN")))
		{
			send2other(hkey, magg);	
			return;
		}

		output(hkey, magg);
	}
		
	void sendOthers() throws IOException
	{
		for(HackKey oneother : otherMap.keySet())
		{
			// Util.pf("\nSending OTHER key=%s, imps = %d", oneother, otherMap.get(oneother).getField(IntFact.impressions));
			output(oneother, otherMap.get(oneother));			
		}
	}
	

	void send2other(HackKey hkey, Metrics magg)
	{
		hkey.setDomain("OTHERS");
		
		if(otherMap.containsKey(hkey))
			{ otherMap.get(hkey).add(magg); }
		else
			{ otherMap.put(hkey, magg); }
	}
	
	void output(HackKey hkey, Metrics magg)
	{
		List<String> vallist = Util.vector();
		
		for(String oneval : hkey.parsemap.values())
			{ vallist.add(oneval); }
		
		for(IntFact ifact : IntFact.values())
		{
			int x = magg.getField(ifact);
			vallist.add(""+x);
		}
		
		for(DblFact dfact : DblFact.values())
		{
			double x = magg.getField(dfact);
			vallist.add(""+x);
		}	
		
		// This is the entry date column, not to be confused with ID_DATE column
		vallist.add(dayCode);
		
		infile.write(Util.join(vallist, "\t"));
		infile.write("\n");	
		outputLines++;
		checkFact.add(magg);
	}
	
	void doUpload()
	{
		double startup = Util.curtime();			
		File campfile = new File(TEMP_INFILE);
		
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			
			int insrows = DatabaseBridge.loadFromFile(campfile, "ad_domain", getColNames(), conn);
			
			conn.close();
			Util.pf("Inserted %d new rows  took %.03f\n", insrows, (Util.curtime()-startup)/1000);
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}
	
	void checkTotals()
	{
		for(IntFact ifact : IntFact.values())
		{
			Util.massert(totalFact.getField(ifact) == checkFact.getField(ifact), "Totals do not agree for field " + ifact);	
		}

		Util.pf("Totals agree, imps=%d, clicks=%d, convs=%d\n",
				totalFact.getField(IntFact.impressions), totalFact.getField(IntFact.clicks), totalFact.getField(IntFact.conversions));
	}
	
	String getCampShardPath() 
	{ 
		return Log2Shard.getCampShardPath(campId, dayCode);	
	}
	
	static BufferedReader sortNRead(String path) throws IOException
	{
		Process p = Runtime.getRuntime().exec("sort " + path);
		BufferedReader sortread = new BufferedReader(new InputStreamReader(p.getInputStream()));
		return sortread;		
	}
	
	static List<String> getDimColNames()
	{
		List<String> clist = Util.vector();
		clist.add("ID_DATE");
		clist.add("ID_CAMPAIGN");
		clist.add("ID_LINEITEM");
		clist.add("NAME_DOMAIN");
		clist.add("ID_EXCHANGE");
		return clist;
	}
	
	static List<String> getColNames()
	{
		List<String> clist = getDimColNames();
		
		for(IntFact intf : IntFact.values())
			{ clist.add("NUM_" + intf.toString().toUpperCase()); }

		for(DblFact dblf : DblFact.values())
			{ clist.add("IMP_" + dblf.toString().toUpperCase()); }
		
		clist.add("ENTRY_DATE");
		
		return clist;
	}	
}
