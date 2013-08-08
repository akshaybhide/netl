package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.DbUtil.*;
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

public class CurrencyInfo
{		
	private static final String EURO_XML_FEED = "http://www.ecb.int/stats/eurofxref/eurofxref-daily.xml";	
	
	List<String> httpdown;
	
	Map<String, Double> euroMap = Util.treemap();
	Map<String, Double> dollMap = Util.treemap();
	
	String dayCode;
	
	SimpleMail logMail;
	
	public static void main(String[] args) throws Exception
	{
		Map<String, String> optargs = Util.getClArgMap(args);
		
		// Basic operation
		if(optargs.size() == 0)
		{			
			CurrencyInfo cinf = new CurrencyInfo();
			cinf.runProcess();
		}
		
		if(optargs.containsKey("rerun"))
		{
			CurrencyInfo cinf = new CurrencyInfo(optargs.get("rerun"));
			cinf.runProcess();
		}
	}
	
	CurrencyInfo()
	{
		this(TimeUtil.getTodayCode());
	}
	
	CurrencyInfo(String dc)
	{
		dayCode = dc;	
		Util.massert(TimeUtil.checkDayCode(dayCode));
		
		logMail = new SimpleMail("ExchangeRateUpdate for " + dayCode);
	}
	
	void runProcess()
	{
		try {
			grabData();	
			parseData();
			updateDatabase();
		} catch (Exception ex) {
			
			logMail.pf("Encountered exception: %s", ex.getMessage());
		}
		
		logMail.send2admin();
	}
	 
	void grabData() throws IOException
	{
		String xmlpath = BmUtil.getCurrencyXmlPath(dayCode);
		if((new File(xmlpath)).exists())
		{
			Util.pf("File %s already exists, skipping download \n", xmlpath);
			return;
		}
		
		List<String> downdata = Util.httpDownload(EURO_XML_FEED);
		FileUtils.writeFileLines(downdata, xmlpath);
		logMail.pf("\nDownloaded %d lines of data from %s\n", downdata.size(), EURO_XML_FEED);
	} 
	
	void updateDatabase()
	{
		List<ConnectionSource> cslist = Util.vector();
		{
			cslist.add(new DatabaseBridge(DbTarget.internal));
			cslist.add(new DatabaseBridge(DbTarget.external));
			cslist.add(NZConnSource.getNetezzaConn("FASTETL"));
		}
		
		// for(DbTarget onetarg : new DbTarget[] { DbTarget.internal, DbTarget.external, DbTarget.new_internal })
		for(ConnectionSource csource : cslist)
		{			
			int insrows = DatabaseBridge.updateExchangeRateInfo(dayCode, dollMap, csource);
			logMail.pf("\nInserted %d rows for daycode=%s for connsource %s\n", insrows, dayCode, csource);
		}
		
		// Also update latest_exchange_rates for internal
		{
			int updated = DatabaseBridge.updateLatestExchangeRates(DbTarget.internal);
			logMail.pf("Updated %d Latest Exchange Rate rows for target %s\n", updated, DbTarget.internal);
		}		
				
	}
	
	void parseData()
	{
		httpdown = FileUtils.readFileLinesE(BmUtil.getCurrencyXmlPath(dayCode));
		Util.pf("\nRead %d lines of xml file", httpdown.size());
		
		euroMap.put("EUR", 1.0);
		
		for(String oneline : httpdown)
		{
			findDateInfo(oneline);
			updateEuroMap(oneline);	
		}

		logMail.pf("\nFound %d total currencies for date %s", euroMap.size(), dayCode);
		
		calcDollarMap();
		
		for(CurrCode cc : CurrCode.values())
		{
			double x = dollMap.get(cc.toString());	
			logMail.pf("\nOne USD buys %.03f in %s", x, cc.toString());
		}
		
	}
	
	void calcDollarMap()
	{
		double eur2usd = euroMap.get("USD");

		for(String ccode : euroMap.keySet())
			{ dollMap.put(ccode, euroMap.get(ccode) / eur2usd); }
	}
	
	void printDollMap()
	{
		for(String ccode : dollMap.keySet())
		{
			Util.pf("\nOne USD buys %.04f %s", dollMap.get(ccode), ccode);	
		}
	}
	
	
	void updateEuroMap(String oneline)
	{
		String currguess = getQuoteAfter(oneline, "currency");
		String rateguess = getQuoteAfter(oneline, "rate");
		
		if(currguess != null)
		{
			// Util.pf("\nFound currency=%s, rate=%s", currguess, rateguess);
			euroMap.put(currguess, Double.valueOf(rateguess));
		}
	}
	
	void findDateInfo(String oneline)
	{
		String dayguess = getQuoteAfter(oneline, "time");
		
		if(dayguess == null)
			{ return; }
		
		Set<String> acceptset = Util.treeset();
		acceptset.addAll(TimeUtil.getDateRange(dayCode, 5));
		
		if(!acceptset.contains(dayguess))
		{ 
			Util.pf("\nFound day code for %s, expecting %s", dayguess, acceptset);
			Util.massert(dayguess.equals(dayCode), "Error processing daycode", dayguess, dayCode);
		}
	}
	
	String getQuoteAfter(String findin, String targid)
	{
		int f = findin.indexOf(targid + "=");
		
		if(f == -1)
			{ return null; }
		
		int rqs = findin.indexOf("'", f);
		
		if(rqs == -1)
			{ return null; }
		
		int rqe = findin.indexOf("'", rqs+1);
		
		return findin.substring(rqs+1, rqe);		
	}
}
