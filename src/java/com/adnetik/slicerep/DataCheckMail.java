
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import java.math.BigInteger;

public class DataCheckMail
{
	private String dayCode;
	private SimpleMail logMail;
	
	public enum CheckField { bidcount, impcount, clickcount };
	
	public static void main(String[] args)
	{
		// doAlert();
		if(args.length < 1)
		{
			Util.pf("Usage: DataCheckMail <daycode|yest>\n");
			return;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		DataCheckMail dcm = new DataCheckMail(daycode);
		dcm.runCheck();
		dcm.logMail.send2admin();

		//checkMap(map_a, map_b);
	}

	public DataCheckMail(String dc)
	{
		logMail = new SimpleMail("Data Check Mail for " + dc);
		dayCode = dc;
	}
	
	void runCheck()
	{
		for(int cid = 1500; cid < 1600; cid++)
		{
			DataChecker dcheck = new DataChecker(cid);
			
			dcheck.setInitial(DbTarget.internal, AggType.ad_general);
			
			dcheck.checkAgainst(DbTarget.internal, AggType.ad_domain);
			dcheck.checkAgainst(DbTarget.external, AggType.ad_domain);			
			
			dcheck.printData();			
			// Util.pf("Finished check for campaign %d\n", cid);
		}		
		
	}
	
	public class DataChecker
	{
		Integer campId;
		Integer initRowCount;
		
		Map<CheckField, Integer> intVals = Util.treemap();
		
		
		public DataChecker(Integer cid)
		{
			campId = cid;
		}
		
		void setInitial(DbTarget dbtarg, AggType atype)
		{
			Map<String, Vector<String>> resmap = getResMap(dbtarg, atype);
			initRowCount = Integer.valueOf(resmap.get("rowcount").get(0));
			
			for(CheckField cf : CheckField.values())
			{
				int myval = getVal(cf, resmap);
				intVals.put(cf, myval);
			}
		}
		
		void printData()
		{
			if(initRowCount == 0)
			{ 
				// Util.pf("No data found for campaign %d\n", campId);	
			} else {
				
				logMail.pf("Found map %s for campaign %d\n", intVals, campId);	
				logMail.pf("----------------------------\n", intVals, campId);	
			}
			
			
		}
		private Integer getVal(CheckField cfield, Map<String, Vector<String>> resmap)
		{
			int rowcount = Integer.valueOf(resmap.get("rowcount").get(0));

			if(rowcount == 0)
				{ return 0; }
			
			return Integer.valueOf(resmap.get(cfield.toString()).get(0));
		}
		
		void checkAgainst(DbTarget dbtarg, AggType atype)
		{
			Map<String, Vector<String>> resmap = getResMap(dbtarg, atype);
			
			for(CheckField cf : CheckField.values())
			{
				int new_val = getVal(cf, resmap);
				int prv_val = intVals.get(cf);
				
				if(new_val != prv_val)
				{
					logMail.pf("ERROR For campid=%d, field %s, found %d, expected %d\n", campId, cf, new_val, prv_val);
				}
			}			
		}
		
		private TreeMap<String, Vector<String>> getResMap(DbTarget dbtarg, AggType atype)
		{
			String tabname = DatabaseBridge.getAggTableName(dbtarg, atype);
			String colsql = Util.sprintf("select count(*) as rowcount, sum(num_bids) as bidcount, sum(num_clicks) as clickcount, sum(num_impressions) as impcount, id_campaign from %s", tabname);
			String fullsql = Util.sprintf("%s where entry_date = '%s' AND id_campaign = %d AND id_exchange != 126", colsql, dayCode, campId);
			
			try {
				Connection conn = DatabaseBridge.getDbConnection(dbtarg);
				TreeMap<String, Vector<String>> resmap = Util.treemap();
				double timesec = DbUtil.sqlQuery2Map(conn, fullsql, resmap);
				conn.close();	
				
				// Util.pf("Query took %.03f seconds\n", timesec);
				
				// showData(resmap);
				return resmap;
			} catch (SQLException sqlex) {
				throw new RuntimeException(sqlex);
			}
		}		
		
		
	}
}
