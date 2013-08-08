
package com.adnetik.bm_etl;

import java.util.*;
import java.io.*;
import java.sql.*;

import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;


public class DomainParse
{
	
	PrintWriter infile;
	int lcount = 0;
	int outputLines = 0;
	static int DOMAIN_LIMIT = 400;
	
	static String TEMP_INFILE = "temp_infile.txt";
	
	LinkedHashMap<String, Integer> topDomMap = new LinkedHashMap<String, Integer>();
	
	Map<String, Metrics> otherMap = Util.treemap();
	
	int campId; 
	String dayCode;
	
	public static void main(String[] args) throws Exception
	{
		for(int cprobe = 0; cprobe < 10000; cprobe++)
		{
			DomainParse dparse = new DomainParse(cprobe, "2012-03-20");
			
			File shardfile = new File(dparse.getCampShardPath());
			if(!shardfile.exists())
				{ continue; }
			
			Util.pf("\nFound a shard file for campid=%d", cprobe);
			dparse.countImps();
			
			//FileUtils.serialize(dparse.topDomMap, "topdommap.ser");
			dparse.doAgg();
			dparse.doUpload();
		}
	}
	
	DomainParse(int cid, String dayc) throws IOException
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
		
		Util.pf("Wrote %d output lines, took %.03f\n", outputLines, (Util.curtime()-startup)/1000);
		
	}
	
	static Pair<HackKey, Metrics> parseLine(String logline)
	{
		int t = 0;
		String[] toks = logline.split("\t");
		
		HackKey hkey = new HackKey();
		Metrics magg = new Metrics();
		
		hkey.setField("campid", toks[t++]);
		hkey.setField("lineitem", toks[t++]);
		hkey.setField("domain", toks[t++]);
		hkey.setField("exchange", toks[t++]);
				
		for(IntFact ifact : IntFact.values())
			{ magg.setField(ifact, Integer.valueOf(toks[t++])); }
		
		for(DblFact dfact : DblFact.values())
			{ magg.setField(dfact, Double.valueOf(toks[t++])); }
		
		return Pair.build(hkey, magg);
	}
	
	static class HackKey
	{
		LinkedHashMap<String, String> parsemap = new LinkedHashMap<String, String>();
		
		public String toString()
		{
			return Util.join(parsemap.values(), "\t");
		}
		
		public void setField(String fname, String data)
		{
			parsemap.put(fname, data);
		}
		
		public String getField(String fname)
		{ 
			return parsemap.get(fname); 
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
		
			String domain = mypair._1.parsemap.get("domain");
			int numimps = mypair._2.getField(IntFact.impressions);

			Util.setdefault(hitmap, domain, 0);
			Util.incHitMap(hitmap, domain, numimps);			
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
				
		Util.pf("\nFinished imp count scan, took %.03f", (Util.curtime()-startup)/1000);
	}

	void send(HackKey hkey, Metrics magg) throws IOException
	{
		if(hkey == null) 
			{ return; }
		
		if(!topDomMap.containsKey(hkey.getField("domain")))
		{
			send2other(hkey, magg);	
			return;
		}

		output(hkey, magg);
	}
		
	void sendOthers() throws IOException
	{
		/*
		for(String oneother : otherMap.keySet())
		{
			HackKey hkey = new HackKey(oneother);
			Util.pf("\nSending OTHER key=%s, imps = %d", oneother, otherMap.get(oneother).getField(IntFact.impressions));
			output(hkey.parsemap, otherMap.get(oneother));			
		}
		*/
	}
	

	void send2other(HackKey hkey, Metrics magg)
	{
		hkey.parsemap.put("domain", "OTHERS");
		String newkey = hkey.toString();
		
		if(otherMap.containsKey(newkey))
			{ otherMap.get(newkey).add(magg); }
		else
			{ otherMap.put(newkey, magg); }
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
		
		infile.write(Util.join(vallist, "\t"));
		infile.write("\n");	
		outputLines++;
	}
	
	void doUpload()
	{
		double startup = Util.curtime();			
		File campfile = new File(TEMP_INFILE);
		
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			
			int insrows = DatabaseBridge.loadFromFile(campfile, "ad_domain", Shard2Db.getColNames(), conn);
			
			conn.close();
			Util.pf("\nInserted %d new rows  took %.03f", insrows, (Util.curtime()-startup)/1000);
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
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
}
