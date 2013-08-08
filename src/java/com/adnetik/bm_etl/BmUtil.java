package com.adnetik.bm_etl;

import java.io.*; 
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.*;

import com.adnetik.shared.Util.*;


/**
* BmUtil
*/
public class BmUtil {
 	
 	public enum DbTarget { internal, external, ext_staging, new_internal, replication };
	
	
	public enum IntFact { bids, clicks, impressions, conversions, conv_post_view, conv_post_click } 
	
	public enum DblFact { bid_amount, cost, cost_euro, cost_pound, deal_price }
		
	// if hasCat = true , there must be a table named "cat_<dimcode>" like cat_country or cat_region
	public enum DimCode { 
		campaign(false), domain(false), date(false), hour(false), quarter(false), lineitem(false), creative(false),
		exchange(true), country(true), metrocode(true), advertiser(false),
		region(true), city(true),  gender(true), browser(true), visibility(true), 
		fbpagetype(false, "unassign_int_2"), publisher(false, "unassign_int_1"),
		size(true), language(true), assignment(false), currcode(true), content(true), utw(false);
		
		private boolean _hasCat;
		
		// This must be the same in both fast_general and fast_domain!!!
		private String _baseTableColName;
		
		DimCode(boolean hc)
		{ this(hc, null); }
		
		DimCode(boolean hc, String btc)
		{
			_hasCat = hc;
			_baseTableColName = btc;	
		}
		
		public boolean hasColumnRename()
		{
			return 	_baseTableColName != null;
		}
		
		public String getBaseTableColName()
		{
			return _baseTableColName == null ? getSmartColName() : _baseTableColName;
		}
		
		public String getSmartColName()
		{
			return "ID_" + this.toString().toUpperCase();
		}
		
		public boolean hasCatalog()
		{
			return _hasCat;
		}
	}		
	
	public enum Counter { PathChecks }
	
	public List<DimCode> getCatDims()
	{	
		List<DimCode> catlist = Util.vector();
		
		for(DimCode dc : DimCode.values())
		{ 
			if(dc.hasCatalog()) { catlist.add(dc); }
		}
		
		return catlist;
	}
	
	public enum CurrCode { USD, GBP, EUR };
	public enum AggType { ad_general, ad_domain };
	
	public static int RMX_EXCHANGE_CODE = 126;
	public static String RMX_UNKNOWN_CODE = "Unknown_RMX";	
	
	public static String BASE_HDFS_DIR = "/bm_etl/";
	
	public static String LOCAL_UTIL_DIR = "/local/fellowship/bm_etl";
	
	public static String getOutputPath(String daycode, DbTarget dbtarg)
	{
		// This is for backwards-compatibility
		String outstr = dbtarg.toString();
		outstr = outstr.substring(0, outstr.length()-2);
		
		String lastdir = outstr + "_" + AggType.ad_general.toString();
		return Util.sprintf("%soutput/%s/%s/", BmUtil.BASE_HDFS_DIR, daycode, lastdir);
	}
	
	static String getCurrencyXmlPath(String dayc)
	{
		return Util.sprintf("/home/burfoot/bm_etl/currency/%s.xml", dayc);
	}	

	
	public static Map<String, String> getParseMap(String stuff)
	{
		Map<String, String> pmap = Util.treemap();
		
		for(String keyval : stuff.split("&"))
		{
			String[] kv = keyval.split("=");
			// Util.massert(kv.length == 2, "Bad key-value pair %s", keyval);
			String mval = (kv.length < 2 ? "NOTSET" : kv[1]);			
			pmap.put(kv[0], mval);
		}
		
		return pmap;
	}		
	
	
	public static Map<String, Integer> mapTimeUserUS;
	public static Map<String, Integer> mapTimeUserCanada;
	public static Map<String, Integer> mapTimeUserCountry;
	
	static {
		initMaps();	
	}
	
	static List<String> getFactNames()
	{
		List<String> flist = Util.vector();
		
		for(IntFact ifact : IntFact.values())
			{ flist.add("NUM_" + ifact.toString().toUpperCase());	}
		
		for(DblFact dfact : DblFact.values())
			{ flist.add("IMP_" + dfact.toString().toUpperCase());	}
		
		return flist;
	}
	
	private static void initMaps()
	{
		
		mapTimeUserUS = Util.conchashmap();
		mapTimeUserUS.put("AL",-1);
		mapTimeUserUS.put("AK",-4);
		mapTimeUserUS.put("AZ",-3);
		mapTimeUserUS.put("AR",-1);
		mapTimeUserUS.put("CA",-3);
		mapTimeUserUS.put("CO",-2);
		mapTimeUserUS.put("CT",0);
		mapTimeUserUS.put("DE",0);
		mapTimeUserUS.put("FL",0);
		mapTimeUserUS.put("GA",0);
		mapTimeUserUS.put("HI",-6);
		mapTimeUserUS.put("ID",-3);
		mapTimeUserUS.put("IL",-1);
		mapTimeUserUS.put("IN",0);
		mapTimeUserUS.put("IA",-1);
		mapTimeUserUS.put("KS",-1);
		mapTimeUserUS.put("KY",0);
		mapTimeUserUS.put("LA",-1);
		mapTimeUserUS.put("ME",0);
		mapTimeUserUS.put("MD",0);
		mapTimeUserUS.put("MA",0);
		mapTimeUserUS.put("MI",0);
		mapTimeUserUS.put("MN",-1);
		mapTimeUserUS.put("MS",-1);
		mapTimeUserUS.put("MO",-1);
		mapTimeUserUS.put("MT",-2);
		mapTimeUserUS.put("NE",-2);
		mapTimeUserUS.put("NV",-3);
		mapTimeUserUS.put("NH",0);
		mapTimeUserUS.put("NJ",0);
		mapTimeUserUS.put("NM",-2);
		mapTimeUserUS.put("NY",0);
		mapTimeUserUS.put("NC",0);
		mapTimeUserUS.put("ND",-1);
		mapTimeUserUS.put("OH",0);
		mapTimeUserUS.put("OK",-1);
		mapTimeUserUS.put("OR",-3);
		mapTimeUserUS.put("PA",0);
		mapTimeUserUS.put("RI",0);
		mapTimeUserUS.put("SC",0);
		mapTimeUserUS.put("SD",-1);
		mapTimeUserUS.put("TN",0);
		mapTimeUserUS.put("TX",-1);
		mapTimeUserUS.put("UT",-2);
		mapTimeUserUS.put("VT",0);
		mapTimeUserUS.put("VA",0);
		mapTimeUserUS.put("WA",-3);
		mapTimeUserUS.put("WV",0);
		mapTimeUserUS.put("WI",-1);
		mapTimeUserUS.put("WY",-2);
		mapTimeUserUS.put("DC",0);
		mapTimeUserUS.put("AS",-7);
		mapTimeUserUS.put("GU",15);
		mapTimeUserUS.put("MP",15);
		mapTimeUserUS.put("PR",0);
		mapTimeUserUS.put("UM",15);
		mapTimeUserUS.put("VI",1);
		
		mapTimeUserCanada = Util.conchashmap();
		mapTimeUserCanada.put("AB",-2);
		mapTimeUserCanada.put("BC",-2);
		mapTimeUserCanada.put("MB",-1);
		mapTimeUserCanada.put("NB",1);
		mapTimeUserCanada.put("NL",1);
		mapTimeUserCanada.put("NS",1);
		mapTimeUserCanada.put("NT",-2);
		mapTimeUserCanada.put("NU",0);
		mapTimeUserCanada.put("ON",0);
		mapTimeUserCanada.put("PE",1);
		mapTimeUserCanada.put("QC",0);
		mapTimeUserCanada.put("SK",-2);
		
		mapTimeUserCountry = Util.conchashmap();
		mapTimeUserCountry.put("BR",1);
		mapTimeUserCountry.put("CO",-1);
		mapTimeUserCountry.put("FR",6);
		mapTimeUserCountry.put("DE",6);
		mapTimeUserCountry.put("IT",6);
		mapTimeUserCountry.put("MX",-1);
		mapTimeUserCountry.put("NL",6);
		mapTimeUserCountry.put("PY",0);
		mapTimeUserCountry.put("ES",6);
		mapTimeUserCountry.put("SE",6);
		mapTimeUserCountry.put("GB",5);
	}
	
	private static int getTimeZoneAdjust(String country, String region)
	{
		if((country.equals("US")) && (mapTimeUserUS.containsKey(region)))
			return mapTimeUserUS.get(region);
		
		if((country.equals("CA")) && (mapTimeUserCanada.containsKey(region)))
			return mapTimeUserCanada.get(region);
		
		if(mapTimeUserCountry.containsKey(country))
			return mapTimeUserCountry.get(country);			
		
		
		return 0;
	}
	
	static void timeZoneConvert(Calendar mycal, String country, String region)
	{
		Integer tzadjust = getTimeZoneAdjust(country, region);		
		mycal.add(Calendar.HOUR_OF_DAY, tzadjust);		
	}
	
	public static String getLastBizDay(String daycode)
	{
		Calendar c = TimeUtil.dayCode2Cal(daycode);
		
		if(c.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
			{ c = TimeUtil.dayBefore(c); }
		
		if(c.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)
			{ c = TimeUtil.dayBefore(c); }
		
		return TimeUtil.cal2DayCode(c);
	}
	
	// public static final String PROPERTY_FILE = "job.property.file";
	
	// public static final String TAB = "\t";
	
	
	//public static String EXCHANGE_RATE_DIR = BASE_HDFS_DIR + "exc_rates";
	//public static String BASE_CAT_HDFS_PATH = BASE_HDFS_DIR + "catinfo";
	//public static String HDFS_PROPS_DIR = BASE_HDFS_DIR + "/props/";
	
	// public static Map<String, Integer> mapPositions;	
	// public static Map<String, Integer> mapLineItem;
	
	//public static Map<String, CurrencyVO> mapCurrency;
	//public static Map<String, CostLineItemVO> mapCostLineItem;
	
	
}