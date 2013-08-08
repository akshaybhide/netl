

package com.adnetik.bm_etl;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;


public class PullRmxData
{
	
	public static final DimCode[] NON_SET_RMX_DIMS = new DimCode[] { 
		DimCode.country, DimCode.metrocode, DimCode.region,
		DimCode.browser, DimCode.visibility,
		DimCode.language
	};
	
	private static Map<DimCode, Map<String, Integer>> _BIG_CAT_MAP = Util.treemap();
	private static Map<DimCode, Integer> _UNK_RMX_CODES = Util.treemap();
	
	
	// This should run AFTER daily Hadoop2Infile, which deletes all the previously entered
	// data for a given day.
	
	public static void main(String[] args) throws IOException
	{		
		Util.pf("Running RMX data pull ... \n");
		List<String> datelist = getTempDateList();

		// Initialize the RMX map		
		for(DimCode onedim : NON_SET_RMX_DIMS)
			{ findUnknownRmxCode(onedim); }
		
		for(String onedate : datelist)
		{
			deleteNInsert(onedate);	
			resetCampaignInfo(onedate);
			pullToAdGeneral(onedate);
		}
		
		//List<Pair<String, Integer>> ipairlist = getDateCampaignPairs();
		//pullCampaignPairs(ipairlist);
		
		// RmxAlert.doAlert();
	}
	
	static List<String> getTempDateList()
	{
		String sql = "SELECT distinct(date) FROM adnetik.rmx_data";
		List<java.sql.Date> sqldate = DbUtil.execSqlQuery(sql, new DatabaseBridge(DbTarget.external));
		List<String> datelist = Util.vector();
		
		for(java.sql.Date onedate : sqldate)
		{
			datelist.add(onedate.toString());
		}
		
		Util.pf("Date list is %s\n", datelist);
		
		return datelist;
	}	
	
	static void deleteNInsert(String daycode)
	{
		{
			String delsql = Util.sprintf("DELETE FROM rmx_data_perm WHERE date = date('%s')", daycode);
			int delrows = DatabaseBridge.execSqlUpdate(delsql, DbTarget.external);
			Util.pf("Deleted %d rows ...", delrows);
		}
		
		{
			String inssql = Util.sprintf("INSERT INTO rmx_data_perm SELECT * FROM adnetik.rmx_data WHERE date = date('%s')", daycode);
			int insrows = DatabaseBridge.execSqlUpdate(inssql, DbTarget.external);
			Util.pf(", inserted %d rows for date %s\n", insrows, daycode);
		}		
	}
	
	
	static void resetCampaignInfo(String daycode)
	{
		// Okay, this is actually two UPDATE/JOIN statements. 
		{
			String nullsql = Util.sprintf("UPDATE rmx_data_perm SET campaign_id = NULL WHERE date = date('%s')", daycode);
			int nullrows = DatabaseBridge.execSqlUpdate(nullsql, DbTarget.external);
			Util.pf("Nulling out campaign id, hit %d rows\n", nullrows);
		}	
		
		{
			String onesql = "UPDATE rmx_data_perm RDP INNER JOIN adnetik.external_line_item ELI on RDP.external_line_item_id = ELI.id  ";
			onesql += Util.sprintf(" SET RDP.campaign_id = ELI.external_campaign_id WHERE ELI.external_campaign_id IS NOT NULL AND RDP.date = date('%s') ", daycode);
			int onerows = DatabaseBridge.execSqlUpdate(onesql, DbTarget.external);
			Util.pf("First campaign info update, hit %d rows\n", onerows);
		}
				
		{
			String twosql = "UPDATE rmx_data_perm RDP INNER JOIN adnetik.external_campaign ECA on RDP.campaign_id = ECA.id  ";
			twosql += Util.sprintf(" SET RDP.campaign_id = ECA.campaign_id WHERE RDP.date = date('%s')", daycode);
			int tworows = DatabaseBridge.execSqlUpdate(twosql, DbTarget.external);	
			Util.pf("Second campaign info update, hit %d rows\n", tworows);
		}
	}
	
	static List<Integer> getCampaignsForDate(String daycode)
	{
		String sql = Util.sprintf("SELECT distinct(campaign_id) FROM rmx_data_perm WHERE date = date('%s') AND campaign_id IS NOT NULL", daycode);
		List<Integer> camplist = DbUtil.execSqlQuery(sql, new DatabaseBridge(DbTarget.external));
		return camplist;
	}
	
	static void pullToAdGeneral(String daycode)
	{
		List<java.math.BigInteger> camplist;
		{
			String sql = Util.sprintf("SELECT distinct(campaign_id) FROM rmx_data_perm WHERE date = date('%s') AND campaign_id IS NOT NULL ORDER BY campaign_id", daycode);
			camplist = DbUtil.execSqlQuery(sql, new DatabaseBridge(DbTarget.external));
		}
		
		for(java.math.BigInteger onecamp : camplist)
		{
			int arows = pullToAdGeneral(daycode, onecamp.intValue());
			int brows = View2Hard.rmxDiffUpdate(daycode, onecamp.intValue(), true);		
			Util.massert(arows == brows, "PUlled %d rmx->adg vs. %d adg->adg_all", arows, brows);
		}
	}
		
	static int pullToAdGeneral(String daycode, int campid)
	{
		{
			String delsql = Util.sprintf("DELETE FROM ad_general WHERE id_date = date('%s') AND id_exchange = %d AND id_campaign = %d ",
								daycode, BmUtil.RMX_EXCHANGE_CODE, campid);
			int delrows = DatabaseBridge.execSqlUpdate(delsql, DbTarget.external);
			Util.pf("Deleted %d rows ... ", delrows);
		}
		
		{
			Map<String, String> convmap = new LinkedHashMap<String, String>();
			{
				convmap.put("ID_DATE", "date");	
				convmap.put("ID_ADVERTISER", "advertiser_id");
				convmap.put("ID_CAMPAIGN", "campaign_id");
				convmap.put("EXT_LINEITEM", "external_line_item_id");
				convmap.put("NUM_IMPRESSIONS", "NUM_IMPRESSIONS");
				convmap.put("NUM_CLICKS", "NUM_CLICKS");
				convmap.put("NUM_CONVERSIONS", "NUM_CONVERSIONS");	
				convmap.put("IMP_COST", "IMP_COST");
				
				// TODO: take these out?
				convmap.put("IMP_COST_EURO", "0.0"); 
				convmap.put("IMP_COST_POUND", "0.0"); 
				
				convmap.put("ID_EXCHANGE", BmUtil.RMX_EXCHANGE_CODE + "");
				
				for(DimCode onedim : NON_SET_RMX_DIMS)
				{
					String colkey = Util.sprintf("ID_%s", onedim.toString().toUpperCase());
					convmap.put(colkey, findUnknownRmxCode(onedim)+"");
				}
			}
			
			String adgfields = Util.join(convmap.keySet(), ",");
			String rmxfields = Util.join(convmap.values(), ",");
			
			String sql = Util.sprintf("INSERT INTO ad_general ( %s ) \n SELECT %s FROM rmx_data_perm WHERE date = date('%s') AND campaign_id = %d ",
				adgfields, rmxfields, daycode, campid);
			
			// Util.pf("\nSQL statement is \n\t%s\n", sql);
			
			int rows = DatabaseBridge.execSqlUpdate(sql, DbTarget.external);
			Util.pf(", and inserted %d rows into ADG for day=%s, campid=%d\n", rows, daycode, campid);
			return rows;
		}
	}
		
	static Integer findUnknownRmxCode(DimCode dim)
	{
		if(!_UNK_RMX_CODES.containsKey(dim))
		{
		
			if(!_BIG_CAT_MAP.containsKey(dim))
			{
				Util.pf("Looking up dimension for %s", dim);
				Map<String, Integer> catmap = DatabaseBridge.readCatIdMap(dim, "name", DbTarget.external);
				_BIG_CAT_MAP.put(dim, catmap);
			}
			
			Map<String, Integer> onemap = _BIG_CAT_MAP.get(dim);
			
			for(String key : onemap.keySet())
			{
				boolean a = key.toLowerCase().indexOf("rmx") > -1;
				boolean b = key.toLowerCase().indexOf("unknown") > -1;
				
				if(a && b)
				{
					Util.pf(" ... found key %s\n", key);
					int unknownid = onemap.get(key);
					_UNK_RMX_CODES.put(dim, unknownid);
					break;
				}
			}
		}
		
		Util.massert(_UNK_RMX_CODES.containsKey(dim), "Could not find Unknown RMX code for dimension %s", dim);
		
		return 	_UNK_RMX_CODES.get(dim);
	}
}
