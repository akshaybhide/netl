package com.adnetik.bm_etl;

import java.util.*;
import java.sql.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*; 
import com.adnetik.shared.BidLogEntry.*; 
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

/**
 * Code to generate the tables
 */
public class TableCreateSqlGen
{	
	AggType _aggType;
	DbTarget _dbTarg;
	
	StringBuffer _thesql = new StringBuffer();
	
	private String _tabSuff;
	
	public TableCreateSqlGen(AggType at, DbTarget dbt)
	{
		this(at, dbt, "__new");	
	}
	
	public TableCreateSqlGen(AggType at, DbTarget dbt, String suff)
	{
		_aggType = at;
		_dbTarg = dbt;
		_tabSuff = suff;
		
		addPrelim();
		
		addGenericIds();
		addSpecialDims();
		addGenericFacts();
		addUnassigned();
		addFinalStuff();
	}
	
	private void addPrelim()
	{
		String prelim = Util.sprintf("CREATE TABLE %s (", getTargTableName());	
		_thesql.append(prelim);
	}
	
	private void addGenericIds()
	{		
		Map<DimCode, String> dimmap = Util.treemap();		
	
		{
			dimmap.put(DimCode.date, "date");
			dimmap.put(DimCode.hour, "tinyint");
			dimmap.put(DimCode.campaign, "mediumint");
			dimmap.put(DimCode.lineitem, "int");
			dimmap.put(DimCode.exchange, "tinyint unsigned");
			dimmap.put(DimCode.currcode, "tinyint unsigned");
			dimmap.put(DimCode.country, "smallint");
			dimmap.put(DimCode.creative, "int");
			dimmap.put(DimCode.content, "tinyint unsigned");
			dimmap.put(DimCode.utw, "smallint unsigned");	
		} 		
		
		for(DimCode onedim : dimmap.keySet())
		{
			String oneline = Util.sprintf("\n\tID_%s\t%s,", onedim.toString().toUpperCase(), dimmap.get(onedim));
			_thesql.append(oneline);
		}
	}
	
	private void addSpecialDims()
	{
		Map<DimCode, String> dimmap = Util.treemap();		
		
		if(_aggType == AggType.ad_general)
		{
			// dimmap.put(DimCode.advertiser, "int");
			dimmap.put(DimCode.metrocode, "smallint");
			dimmap.put(DimCode.region, "smallint");
			dimmap.put(DimCode.size, "smallint");
			dimmap.put(DimCode.visibility, "smallint");
			dimmap.put(DimCode.browser, "smallint");
			dimmap.put(DimCode.language, "smallint");
			
		} else {
			dimmap.put(DimCode.domain, "varchar(255)");
			// dimmap.put(DimCode.is_cc, "tinyint");
		}
		

		for(DimCode onedim : dimmap.keySet())
		{
			String oneline = Util.sprintf("\n\tID_%s\t%s,", onedim.toString().toUpperCase(), dimmap.get(onedim));	
			_thesql.append(oneline);
		}
		
	}
	
	private void addGenericFacts()
	{
		SortedMap<IntFact, String> intmap = Util.treemap();
		{
			intmap.put(IntFact.bids, "int unsigned");
			intmap.put(IntFact.impressions, "mediumint unsigned");
			intmap.put(IntFact.clicks, "smallint unsigned");
			intmap.put(IntFact.conversions, "smallint unsigned");
			intmap.put(IntFact.conv_post_view, "smallint unsigned");
			intmap.put(IntFact.conv_post_click, "smallint unsigned");
		}
		
		for(IntFact ifact : intmap.keySet())
		{
			String oneline = Util.sprintf("\n\tNUM_%s\t%s,", ifact.toString().toUpperCase(), intmap.get(ifact));			
			_thesql.append(oneline);
		}
		
		for(DblFact dfact : new DblFact[] { DblFact.cost, DblFact.bid_amount })
		{
			String oneline = Util.sprintf("\n\tIMP_%s\tdouble,", dfact.toString().toUpperCase());			
			_thesql.append(oneline);
		}		
	}
	
	private void addUnassigned()
	{
		for(int i = 0; i < 4; i++)
		{
			String oneline = Util.sprintf("\n\tunassign_text_%d\tvarchar(128),", i);	
			_thesql.append(oneline);
		}

		String[] int_type = new String[] { "int", "mediumint", "smallint", "tinyint unsigned", "tinyint unsigned" };
		
		
		for(int i = 0; i < int_type.length; i++)
		{
			String oneline = Util.sprintf("\n\tunassign_int_%d\t%s,", i, int_type[i]);	
			_thesql.append(oneline);
		}		
		
		_thesql.append("\n\tunassign_decimal\tdecimal(10,4),");
		
	}
	
	private String getTargTableName()
	{
		return DatabaseBridge.getAggTableName(_dbTarg, _aggType) + _tabSuff;		
	}
	
	private void addFinalStuff()
	{
		if(_aggType == AggType.ad_domain)
		{ 
			_thesql.append("\n\tHAS_CC\ttinyint unsigned,");
			_thesql.append("\n\tRAND99\ttinyint unsigned,");
			_thesql.append("\n\tKEY idx_campaign_cc_rand (ID_CAMPAIGN, HAS_CC, RAND99),");
		}
		
		_thesql.append("\n\tENTRY_DATE\tdate,");
		_thesql.append("\n\tEXT_LINEITEM\tbigint(20) unsigned )");		
		
		if(_aggType == AggType.ad_general)
		{
			_thesql.append("\nPARTITION BY hash(ID_CAMPAIGN) PARTITIONS 1024; ");	
		}
		
		if(_aggType == AggType.ad_domain)
		{
			_thesql.append("\nPARTITION BY hash(DAYOFYEAR(ID_DATE)) PARTITIONS 366; ");	
		}		
		
		// Create indexes afterwards
		// _thesql.append(Util.sprintf("\n CREATE INDEX date_idx ON %s (ID_DATE);", getTargTableName()));
		
	}
	
	String getCreateSql()
	{
		return _thesql.toString();
	}

	

	
	public static void main(String[] args)
	{
		AggType mytype = AggType.valueOf(args[0]);
		
		String suff = args[1];
		
		TableCreateSqlGen tcsg = new TableCreateSqlGen(mytype, DbTarget.internal, suff);

		Util.pf("create sql is : \n%s\n", tcsg.getCreateSql());
	}
}
