package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*; 

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

/**
 * 
 * CatalogUtil
 * 
 */
public class CatalogUtil
{	
	private static CatalogUtil _SING;
	
	private DbTarget _dbTarg;
	
	
	// Log code lookups for a bunch of fields
	Map<DimCode, Map<String, Integer>> logCodeMap = Util.treemap();
	
	Map<CurrCode, Double> excRateMap;
		
	// Default maps
	private Map<DimCode, Integer> unknownMap = Util.conchashmap();
	private Map<DimCode, Integer> othersMap = Util.conchashmap();
	
	private Map<AggType, SortedSet<DimCode>> _dimSetMap;
	
	private Map<Integer, Integer> appnexsCtvLookup = Util.treemap();
	private Map<Integer, Integer> regularCtvLookup = Util.treemap();
	
	// Returns singleton instance of CatalogUtil
	// Need daycode information to 
	public static synchronized CatalogUtil getSing()
	{
		if(_SING == null)
			{ throw new RuntimeException("CatalogUtil is not initialized, must called initSing(..) before getSing()"); }
		
		return _SING;
	}
	
	public static synchronized boolean isSingReady()
	{
		return (_SING != null);	
	}
	
	public static synchronized double initSing(DbTarget dbtarg)
	{
		return initSing(null, dbtarg);	
	}
		
	public static synchronized double initSing(String daycode, DbTarget dbtarg)
	{	
		double spendtime = 0;
		
		if(_SING == null)
		{
			double startup = Util.curtime();
			_SING = new CatalogUtil(dbtarg);
			_SING.getDimSetMap();
			
			_SING.readLogCodeMap();
			
			if(daycode != null)
				{ _SING.readExcRateInfo(daycode); }
			
			_SING.readCreativeLookupMaps();
			_SING.readDefaultMaps();
			
			// This is a quick name/type check
			if(dbtarg == DbTarget.internal)
				{  DatabaseBridge.checkStage2MainNameOverlap(dbtarg); }
			
			spendtime = Util.curtime() - startup;
		} else {
			Util.pf("CatalogUtil already initialized.\n");	
		}
		
		return spendtime;
	}
	
	CatalogUtil(DbTarget dbt)
	{
		_dbTarg = dbt;
	}
		
	public Integer getUnknownCode(DimCode dcode)
	{
		return unknownMap.get(dcode);
	}
	
	public Integer getOthersCode(DimCode dcode)
	{
		return othersMap.get(dcode);
	}
	
	void readLogCodeMap() {
	
		for(DimCode onedim : DimCode.values())
		{ 
			if(onedim.hasCatalog())
				{ readCatCodeMap(onedim); }
		}
	}
	
	private void readDefaultMaps()
	{
		DatabaseBridge.populateDefMap(unknownMap, "unknown", _dbTarg);
		DatabaseBridge.populateDefMap(othersMap, "others", _dbTarg);
		
		Util.pf("Found %d default values for unknown\n", unknownMap.size());
		Util.pf("Found %d default values for others\n", othersMap.size());
	}
		
	private void readCatCodeMap(DimCode mdim)
	{
		Util.massert(mdim.hasCatalog(), "Dimension %s does not have a catalog", mdim);
	
		// TODO: this should be isinternal vs. isexternal
		Map<String, Integer> catmap = DatabaseBridge.readCatalog(mdim, _dbTarg);
	
		logCodeMap.put(mdim, catmap);
	}
	
	private void readCreativeLookupMaps()
	{
		String[] keyfield = new String[] { "id", "appnexus_id" };
		
		for(int i = 0; i < 2; i++)
		{
			String sql = Util.sprintf("SELECT %s, creative_id FROM adnetik.assignment WHERE %s IS NOT NULL", keyfield[i], keyfield[i]);
			List<Pair<Number, Number>> idlist = DbUtil.execSqlQueryPair(sql, new DatabaseBridge(_dbTarg));
			fillMapFromList(idlist, (i == 0 ? regularCtvLookup : appnexsCtvLookup));			
		}
	}
	
	private void fillMapFromList(List<Pair<Number, Number>> listinfo, Map<Integer, Integer> targmap)
	{
		for(Pair<Number, Number> onepair : listinfo)
		{
			int k = onepair._1.intValue();
			int v = onepair._2.intValue();
			
			if(targmap.containsKey(k))
				{ Util.massert(k == 0 || targmap.get(k) == v, "Error, inconsistency in creative lookup map"); }
			
			targmap.put(k, v);
		}
		
		// Util.pf("Read %d distinct keys for map, total of %d\n", targmap.size(), listinfo.size());
	}
	
	// TODO: this should actually not be part of CatalogUtil
	void readExcRateInfo(String daycode) 
	{
		String reldaycode = BmUtil.getLastBizDay(daycode);

		Util.pf("\nPulling exchange rate info, day is %s, rel-day is %s", daycode, reldaycode); 
		
		excRateMap = DatabaseBridge.readExchangeRateInfo(reldaycode, _dbTarg);
		
		Util.pf(" ... done, map is %s", excRateMap);
		
		if(!(excRateMap.containsKey(CurrCode.EUR) && (excRateMap.containsKey(CurrCode.GBP))))
		{
			throw new RuntimeException("Could not find valid exchange rate info for daycode " +  daycode);
		}
	}

	// Converts a currency amount denominated in a source currency to the destination currency
	double convertA2B(CurrCode src, CurrCode dst, double a)
	{
		double usd = a / excRateMap.get(src);
		return usd * excRateMap.get(dst);
	}
	
	/*
	NO longer use this because we now have currency in the log record itself
	CurrCode getCurrencyFromPathInfo(PathInfo rpath)
	{
		if(!currCodeExcMap.containsKey(rpath.pExc))
			{ return CurrCode.USD; }
		
		CurrCode x = currCodeExcMap.get(rpath.pExc).get(rpath.pCenter);
		
		return (x == null ? CurrCode.USD : x);
	}
	*/
	
	public Integer lookupCreativeId(int asstOrAppnId, boolean isappnexus)
	{
		Map<Integer, Integer> relmap = (isappnexus ? appnexsCtvLookup : regularCtvLookup);
		Integer result = relmap.get(asstOrAppnId);
		//Util.massert(result != null, "Could not find CTV ID for (%s) ID=%d", (isappnexus ? "appnexus" : "assignment"), asstOrAppnId);
		return (result == null ? -1 : result);
	}
	
	public Map<AggType, SortedSet<DimCode>> getDimSetMap()
	{
		if(_dimSetMap == null)
		{
			_dimSetMap = Util.treemap();
			
			for(AggType onetype : AggType.values())
			{
				SortedSet<DimCode> dimset = DatabaseBridge.getDimSet(onetype, _dbTarg);
				_dimSetMap.put(onetype, dimset);
			}
		}
		
		return _dimSetMap;
	}
	
	// "smart" refresh of the creative lookup information
	// Sends a SQL query to AdBoard
	public synchronized void refreshCreativeLookup()
	{
		String assign_sql = "SELECT id, creative_id, appnexus_id FROM adnetik.assignment";
		
		SmartAdPuller sap = new SmartAdPuller(assign_sql);
		
		// TODO: make this skip Mysql headers
		try { sap.runQuery(true); } 
		catch (Exception ex) {  throw new RuntimeException(ex); }	
		
		for(String errline : sap.getErrorLines())
		{
			Util.pf("ERROR: %s\n", errline);	
			
		}
		
		int prevreg = regularCtvLookup.size();
		int prevapp = appnexsCtvLookup.size();
		
		// First line is SQL column headers
		for(int i = 1; i < sap.getOutputLines().size(); i++)
		{
			String resline = sap.getOutputLines().get(i);
			String[] toks = resline.split("\t");
			
			int asstid = Integer.valueOf(toks[0]);
			int crtvid = Integer.valueOf(toks[1]);
			
			regularCtvLookup.put(asstid, crtvid);			
			
			if(!toks[2].equals("NULL"))
			{
				int appnid = Integer.valueOf(toks[2]);
				appnexsCtvLookup.put(appnid, crtvid);
			}
		}
		
		Util.pf("Refreshed creative lookup from %d total lines, found %d new regulars, %d new appnexus\n",
			sap.getOutputLines().size(), regularCtvLookup.size()-prevreg, appnexsCtvLookup.size()-prevapp);
	}
}