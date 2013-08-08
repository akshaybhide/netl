
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.BmUtil.*;


public class ActLogTest
{
	public static void main(String[] args) throws IOException
	{
		for(DimCode dcode : DimCode.values())
		{
			Util.pf("Base table is %s for DimCode %s\n",
				dcode.getBaseTableColName(), dcode);
			
		}
		
		/*
		List<String> mylist = Util.getNfsLogPaths(ExcName.rtb, LogType.activity, "2013-03-01");
		
		for(String onepath : mylist)
		{
			// PathInfo pathinfo = new PathInfo(onepath);
			
	
			// Util.pf("One path is %s\n", onepath);
			
			// Util.pf("Path info is %s\n", pathinfo);
			
			checkPath(onepath);
			
			// break;
		}
		*/
		
		// checkLogInfo();
		
	}
	
	private static void checkLogInfo()
	{
		Map<LogField, Integer> convmap = FieldLookup.getFieldMap(LogType.conversion, LogVersion.v21); 	
		Map<LogField, Integer> actvmap = FieldLookup.getFieldMap(LogType.activity  , LogVersion.v21); 		
		
		for(LogField lf : convmap.keySet())
		{
			Util.massert(actvmap.containsKey(lf),
				"LogField %s not found in actvmap", lf);
			
			int a = convmap.get(lf);
			int b = actvmap.get(lf);
			
			Util.pf("LF=%s, conv=%d, actv=%d\n", lf, convmap.get(lf), actvmap.get(lf));
			
			Util.massert(a == b,
				"LF %s is in %d for conv map but %d for actv map",
				lf, convmap.get(lf), actvmap.get(lf));
		}
	}
	
	private static void checkPath(String onepath) throws IOException
	{
		
		PathInfo pathinfo = new PathInfo(onepath);
		
		BufferedReader bread = FileUtils.getGzipReader(onepath);
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			try { 
				// BidLogEntry ble = new BidLogEntry(LogType.conversion, pathinfo.pVers, oneline);
				BidLogEntry ble = new BidLogEntry(LogType.activity, pathinfo.pVers, oneline);
				
				// BidLogEntry ble = new BidLogEntry(pathinfo.pType, pathinfo.pVers, oneline);
				String wtpid = ble.getField(LogField.wtp_user_id);
				int campid = ble.getIntField(LogField.campaign_id);
				int lineid = ble.getIntField(LogField.line_item_id);
				
				// LogField lf = LogField.use
				
				String udma = ble.getField(LogField.user_DMA);
				String lang = ble.getField(LogField.user_language);
				
				Util.pf("Campaign ID = %d, WTP ID is %s\n", campid, wtpid);
				
				Util.pf("DMA is %s, lang is %s\n", udma, lang);
				
			} catch (BidLogFormatException blex) {
				
				blex.printStackTrace();	
			}
		}
		
		bread.close();
		
	}
}
