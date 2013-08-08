
package com.adnetik.bm_etl;

import java.util.*;
import java.io.*;
import java.sql.*;

import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;


public class Log2Shard
{
	static String[] FIELD_NAMES = new String[] { "campaign_id", "line_item_id", "domain" };
	
	// static int DOMAIN_LIMIT = 400;
	
	private Map<Integer, PrintWriter> writerMap = Util.treemap();
	
	String dayCode;
	
	Map<String, Integer> exchangeCat;
	// Map<String, Integer> domainCat = Util.treemap();
		
	public static void main(String[] args) throws Exception
	{
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		Map<String, String> optargs = Util.getClArgMap(args);
		
		Log2Shard l2s = new Log2Shard(daycode);
		Integer maxfile = optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : Integer.MAX_VALUE;
				
		int fCount = 0;
		List<String> pathlist = getLogPaths(daycode);
		Collections.shuffle(pathlist);
		double fstart = Util.curtime();
		
		// Pull info from database
		CatalogUtil.initSing(daycode, DbTarget.external);
		
		for(int i = 0; i < pathlist.size() && i < maxfile; i++)
		{
			double startup = Util.curtime();
			String onepath = pathlist.get(i);
			l2s.processLogFile(onepath, daycode);
			double endtime = Util.curtime();
			Util.pf("\nProcessing path %d/%d, took %.03f secs, total %.03f, avg=%.03f/file", 
				i, pathlist.size(), (endtime-startup)/1000, (endtime-fstart)/1000, (endtime-fstart)/(1000*(i+1)));		
		}
		
		l2s.closeWriters();		
	}
	
	Log2Shard(String dayc) throws SQLException
	{
		exchangeCat = DatabaseBridge.readCatalog(DimCode.exchange, DbTarget.external);
		dayCode = dayc;
		Util.pf("\nFound %d exchanges in catalogs", exchangeCat.size());
	}
	
	PrintWriter getMyWriter(int campid)
	{
		try {
			String camppath = getCampShardPath(campid, dayCode);
			File parentfile = (new File(camppath)).getParentFile();
			parentfile.mkdirs();			
			return new PrintWriter(camppath);	
		} catch (FileNotFoundException ffex) {
			throw new RuntimeException(ffex); 	
		}
	}	
	
	Map<String, String> log2colMap(BidLogEntry ble)
	{
		Map<String, String> colmap = Util.hashmap();
		
		// eg 2012-03-15 = 10 characters
		colmap.put("ID_DATE", ble.getField("date_time").trim().substring(0, 10));
		colmap.put("ID_CAMPAIGN", ble.getField("campaign_id"));
		colmap.put("NAME_DOMAIN", ble.getField("domain"));
		colmap.put("ID_LINEITEM", ble.getField("line_item_id"));
		
		String excname = ble.getField("ad_exchange");
		colmap.put("ID_EXCHANGE", exchangeCat.get(excname)+"");
		return colmap;
	}
	
	void closeWriters()
	{
		for(PrintWriter pw : writerMap.values())
			{ pw.close(); }
	}
	
	static String getCampShardPath(int campid, String daycode)
	{
		return Util.sprintf("/mnt/data/burfoot/bm_etl/camp_shards/%s/camp_%d.txt", daycode, campid);	
	}
	
	void processLogFile(String fpath, String daycode) throws IOException
	{
		int lcount = 0;
		PathInfo loginfo = new PathInfo(fpath);
		BufferedReader bread = Util.getGzipReader(fpath);
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			BleStructure bles = null; 
			
			// TODO: autolookup of LogVersion
			try { bles = BleStructure.buildStructure(loginfo.pType, loginfo.pVers, oneline); }
			catch (BidLogFormatException blex) { continue; }
			
			int campid = bles.logentry.getIntField("campaign_id");
			if(!writerMap.containsKey(campid))
			{	 
				writerMap.put(campid, getMyWriter(campid));
			}
			PrintWriter cwrite = writerMap.get(campid);			
			
			// Dimensions 
			{
				List<String> dlist = Util.vector();
				Map<String, String> colmap = log2colMap(bles.logentry);
				
				for(String onecol : Shard2Db.getDimColNames())
				{ 
					dlist.add(colmap.get(onecol)); 
				}
				
				//Util.pf("\nDlist is %s", dlist);
				cwrite.write(Util.join(dlist, ","));
			}
			
			cwrite.write("\t");
			
			// Facts
			{
				Metrics onemet = bles.returnMetrics();
				onemet.updateCostCurrencyInfo(loginfo);
				cwrite.write(onemet.toString(",", false));
			}
			
			cwrite.write("\n");			
			lcount++;
		}
		
		bread.close();
		// Util.pf("\nWrote %d lines for file \n\t%s", lcount, fpath);
	}


	
	// Why isn't this a standard BmUtil operation?
	static List<String> getLogPaths(String daycode)
	{
		List<String> plist = Util.vector();
		LogType[] touse = new LogType[] { LogType.imp, LogType.conversion, LogType.click };
		
		for(ExcName exc : ExcName.values())
		{
			for(LogType reltype : touse)
			{
				List<String> sublist = Util.getNfsLogPaths(exc, reltype, daycode);
				
				if(sublist == null)
					{ Util.pf("\nWarning, no log files found for excname %s, daycode=%s", exc, daycode); }
				else 
					{ plist.addAll(sublist); }
			}
		}

		return plist;
	}
}
