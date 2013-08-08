/**
 * 
 */
package com.adnetik.bm_etl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


public class View2Hard 
{ 
	public static final String TARG_TABLE_GENERAL = "ad_general_all";
	public static final String VIEW_NAME_GENERAL = "v__ad_general_all";
	
	public static final String TARG_TABLE_DOMAIN = "ad_domain_cpm";
	public static final String VIEW_NAME_DOMAIN = "v__ad_domain_all";
	
	public static String TMP_SUFF = "__tmp";
	public static String OLD_SUFF = "__old";
	public static String CUR_SUFF = "__tst";
	
	public static void main(String[] args) throws Exception
	{ 
		Map<String, String> clargs = Util.getClArgMap(args);
		
		if(clargs.containsKey("campid")) {

			Integer campid = Integer.valueOf(clargs.get("campid"));
			fullCampaignUpdate(campid);
		} 
		else if(clargs.containsKey("daycode")) {
			
			String daycode = clargs.get("daycode");
			daycode = ("yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode);
			FullDayUpdater fdup = new FullDayUpdater(daycode);
			fdup.doUpdate();				
		}
		else {
			Util.pf("\nMust specify daycode or campaign ID\n");	
			return;
		}
	}
	
	static class FullDayUpdater
	{
		String _dayCode;
		Set<Integer> _campSet = Util.treeset();
		SimpleMail _logMail = null;
		
		public FullDayUpdater(String dc)
		{
			_dayCode = dc;
			_logMail = new SimpleMail("View2Hard Full Day Update for " + _dayCode);			
		}
		
		public void doUpdate()
		{
			grabIdSet();
			diffUpdate(_campSet, _dayCode, true, _logMail);
			diffUpdate(_campSet, _dayCode, false, _logMail);			
			_logMail.send2admin();
		}
		
		private void grabIdSet()
		{
			_logMail.pf("Querying campaign IDs...");
			String sql = Util.sprintf("SELECT DISTINCT(id_campaign) FROM ad_domain WHERE entry_date = '%s' ORDER BY id_campaign", _dayCode);
			List<Integer> idlist = DbUtil.execSqlQuery(sql, new DatabaseBridge(DbTarget.external));
			_logMail.pf(" ... done, found %d IDs: %s\n", idlist.size(), idlist);
			_campSet.addAll(idlist);			
		}
	}
	
	static void swapNGo()
	{
		String oldtable = "ad_general" + OLD_SUFF;
		String tmptable = "ad_general" + TMP_SUFF;
		String trgtable = "ad_general" + CUR_SUFF;
		
		{
			String rensql = Util.sprintf("RENAME TABLE %s TO %s", trgtable, oldtable);
			execWithTime(rensql, "RENAME trg-->old");
		}	
		
		{
			String rensql = Util.sprintf("RENAME TABLE %s TO %s", tmptable, trgtable);
			execWithTime(rensql, "RENAME tmp-->trg");
		}		
	}
	
	static void probeNPull(int probeid)
	{
		//Util.pf("Probe and pull for %d\n", probeid);
		
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			boolean probehit = false;
			
			// probe
			{
				String qsql = Util.sprintf("SELECT id_campaign FROM ad_general WHERE id_campaign = %d LIMIT 1", probeid);
				List<Number> hitlist = DbUtil.execSqlQuery(qsql, conn);
				
				if(!hitlist.isEmpty())
				{
					Util.massert(hitlist.size() == 1);
					Util.massert(hitlist.get(0).intValue() == probeid);
					// Util.pf("Found campaign ID %d\n", probeid);
					probehit = true;
				}
			}
			
			// pull
			if(probehit) 
			{
				Util.pf("Running update for campaign %d ...", probeid);
				double startup = Util.curtime();
				String insql = Util.sprintf("INSERT INTO ad_general__tmp SELECT * FROM v__ad_general_all WHERE id_campaign = %d", probeid);
				int inrows = DatabaseBridge.execSqlUpdate(insql, conn);
				Util.pf(" ... done, inserted %d rows, took %.03f seconds\n", inrows, (Util.curtime()-startup)/1000);
			}
			
			conn.close();	
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
		}
	}
	
	static void setupTempTable()
	{
		String tmptable = "ad_general" + TMP_SUFF;
		String oldtable = "ad_general" + OLD_SUFF;
		
		if(!Util.checkOkay(Util.sprintf("Delete old table %s", oldtable)))
		{
			System.exit(1);
		}	
		
		//Util.pf("\nGoing to delete old table");
		//System.exit(1);
		
		// Delete old old table
		{
			String delold = Util.sprintf("DROP TABLE IF EXISTS %s ", oldtable);
			execWithTime(delold, "DELETE OLD");			
		}
		
		// Create the table
		{
			String crsql = Util.sprintf("CREATE TABLE %s AS SELECT * from v__ad_general_all LIMIT 1", tmptable);
			execWithTime(crsql, "TABLE CREATE");
		}
		
		// Set partitions
		{
			String prtsql = Util.sprintf("ALTER TABLE %s PARTITION BY HASH(id_campaign) PARTITIONS 1024", tmptable);
			execWithTime(prtsql, "TABLE PARTITION");			
		}	
		
		// Delete single row
		{
			String delsql = Util.sprintf("DELETE FROM %s LIMIT 1", tmptable);
			execWithTime(delsql, "ROW DELETE");			
		}		
	}	
	
	static int execWithTime(String sql, String opcode)
	{
		double startup = Util.curtime();
		Util.pf("Runnning operation: %s ", opcode);
		int rows = DatabaseBridge.execSqlUpdate(sql, DbTarget.external);
		Util.pf(" ... done, %d rows affected, took %.03f\n", rows, (Util.curtime()-startup)/1000);
		return rows;
	}
	
	static int execWithTime(String sql, String opcode, Connection conn) throws SQLException
	{
		double startup = Util.curtime();
		Util.pf("Runnning operation: %s ", opcode);
		int rows = DatabaseBridge.execSqlUpdate(sql, conn);
		Util.pf(" ... done, %d rows affected, took %.03f\n", rows, (Util.curtime()-startup)/1000);
		return rows;
	}	
	
	
	// TODO: hope this is not being used
	static Set<Integer> probeCampaignSet()
	{
		Set<Integer> campset = Util.treeset();
		
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			for(int probeid = 0; probeid < 4000; probeid++)
			{
				String qsql = Util.sprintf("SELECT id_campaign FROM ad_general WHERE id_campaign = %d LIMIT 1", probeid);
				List<Number> hitlist = DbUtil.execSqlQuery(qsql, conn);
				
				if(!hitlist.isEmpty())
				{
					Util.massert(hitlist.size() == 1);
					Util.massert(hitlist.get(0).intValue() == probeid);
					campset.add(probeid);
					Util.pf("Found campaign ID %d\n", probeid);
				}
			}
			conn.close();
			
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);
		}
		
		return campset;
	}
		
	static String getDateCampWhereClause(Integer campid, String daycode)
	{
		List<String> xlist = Util.vector();
		if(campid != null)
			{ xlist.add(" ID_CAMPAIGN = " + campid); }
		
		if(daycode != null)
			{ xlist.add(Util.sprintf(" ID_DATE = date('%s') ", daycode)); }
		
		return Util.sprintf(" WHERE %s ", Util.join(xlist, " AND "));
	}
	
	static Set<String> getFieldOverlapSet(String table_a, String table_b)
	{
		Set<String> colset = Util.treeset();
		
		//List<String> alist = DatabaseBridge.getTableColNames(table_a, DbTarget.external);
		//List<String> blist = DatabaseBridge.getTableColNames(table_b, DbTarget.external);

		// Util.pf("Column list for table %s: \n%s\n", table_a, alist);
		// Util.pf("Column list for table %s: \n%s\n", table_b, blist);
		// System.exit(1);

		colset.addAll(DatabaseBridge.getTableColNames(table_a, DbTarget.external));
		colset.retainAll(DatabaseBridge.getTableColNames(table_b, DbTarget.external));		
		return colset;
	}
	
	static void fullCampaignUpdate(Integer campid)
	{
		if(!Util.checkOkay(Util.sprintf("Delete and replace ALL data for campaign=%d", campid)))
		{ 
			Util.pf("Aborted\n");
			return;
		}
		
		fullCampaignUpdate(campid, TARG_TABLE_GENERAL, VIEW_NAME_GENERAL);
		fullCampaignUpdate(campid, TARG_TABLE_DOMAIN, VIEW_NAME_DOMAIN);
	}
	
	private static void fullCampaignUpdate(Integer campid, String targtable, String viewname)
	{
		Set<String> colset = getFieldOverlapSet(targtable, viewname);
		String whclause = getDateCampWhereClause(campid, null);
		
		{
			String delsql = Util.sprintf("DELETE FROM %s  %s" , targtable, whclause);
			// Util.pf("\nDel sql is %s", delsql);
			execWithTime(delsql, Util.sprintf("DELETE CAMP %d", campid));
		}
		
		{
			String colstr = Util.join(colset, ",");
			String movsql = Util.sprintf("INSERT INTO %s \n(%s) \nSELECT %s \nFROM %s  %s",
				targtable, colstr, colstr, viewname, whclause);
			// Util.pf("\nMove sql is \n%s \n", movsql);
			execWithTime(movsql, Util.sprintf("TRANSFER CAMP %d", campid));
		}			
	}
	
	static int rmxDiffUpdate(String daycode, Integer campid, boolean isgeneral)
	{
		if(isgeneral)
			{ return rmxDiffUpdate(daycode, campid, TARG_TABLE_GENERAL, VIEW_NAME_GENERAL); }
		else 
			{ return rmxDiffUpdate(daycode, campid, TARG_TABLE_DOMAIN, VIEW_NAME_DOMAIN); }
	}
	
	private static int rmxDiffUpdate(String daycode, Integer campid, String targtable, String viewname)
	{
		int rows;
		Set<String> colset = getFieldOverlapSet(targtable, viewname);
		String whclause = getDateCampWhereClause(campid, daycode);
		whclause += " AND id_exchange = " + BmUtil.RMX_EXCHANGE_CODE;
		
		{
			String delsql = Util.sprintf("DELETE FROM %s %s", targtable, whclause);
			// Util.pf("\nDel sql is %s", delsql);
			execWithTime(delsql, Util.sprintf("RMX DELETE CAMP %d", campid));
		}
		
		{
			String colstr = Util.join(colset, ",");
			String movsql = Util.sprintf("INSERT INTO %s \n(%s) \nSELECT %s \nFROM %s  %s",
				targtable, colstr, colstr, viewname, whclause);
			// Util.pf("\nMove sql is \n%s \n", movsql);
			rows = execWithTime(movsql, Util.sprintf("RMX TRANSFER CAMP %d", campid));
		}		
		
		Util.pf("Updated %d rows for RMX diff update for campid=%d, daycode=%s\n", rows, campid, daycode);
		return rows;		
	}
	
	// TODO: third argument here should be an AggType
	static void diffUpdate(Set<Integer> campset, String daycode, boolean isgeneral, SimpleMail logmail)
	{
		if(logmail == null)
			{ logmail = new SimpleMail("GIMP"); }
		
		if(isgeneral)
			{ diffUpdate(campset, daycode, TARG_TABLE_GENERAL, VIEW_NAME_GENERAL, logmail); }
		else 
			{ diffUpdate(campset, daycode, TARG_TABLE_DOMAIN, VIEW_NAME_DOMAIN, logmail); }
	}
	
	private static void diffUpdate(Set<Integer> campset, String daycode, String targtable, String viewname, SimpleMail logmail)
	{
		Set<String> colset = getFieldOverlapSet(targtable, viewname);
		
		try {
			int compcount = 0;
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);

			for(int campid : campset)
			{
				String whclause = getDateCampWhereClause(campid, daycode);
				
				{
					String delsql = Util.sprintf("DELETE FROM %s %s", targtable, whclause);
					// Util.pf("\nDel sql is %s", delsql);
					DbUtil.execWithTime(delsql, Util.sprintf("DELETE CAMP %d", campid), conn, logmail);
				}
				
				{
					String colstr = Util.join(colset, ",");
					String movsql = Util.sprintf("INSERT INTO %s \n(%s) \nSELECT %s \nFROM %s  %s",
									targtable, colstr, colstr, viewname, whclause);
					// Util.pf("\nMove sql is \n%s \n", movsql);
					DbUtil.execWithTime(movsql, Util.sprintf("TRANSFER CAMP %d", campid), conn, logmail);
				}		
				
				compcount++;
				logmail.pf("Finished %d out of %d", compcount, campset.size());
			}
			
			conn.close();
			
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);
		}		
	}
	
}
