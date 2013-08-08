
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.slicerep.SliUtil.*;

public class SliDatabase implements DbUtil.ConnectionSource
{	
	public static int DEL_BATCH = Integer.MAX_VALUE;
	// public static int DEL_BATCH = 100000;
	
	public enum CLarg { rebuildalias, testdelete, showtables };
	
	public static void main(String[] args) throws SQLException
	{
		CLarg myarg = CLarg.valueOf(args[0]);
		
		if(myarg == CLarg.showtables)
		{
			Connection myconn = getConnection();
			List<String> tablist = DbUtil.showTables(myconn);
			myconn.close();
			Util.pf("Table list is %s\n", tablist);			
		}	
		
		if(myarg == CLarg.testdelete)
		{
			String daycode = args[1];
			Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode %s", daycode);
			limitDeleteOld(daycode, AggType.ad_domain);
		}
	}
		
	public static Connection getConnection() throws SQLException
	{
		return DatabaseBridge.getDbConnection(DbTarget.internal);
	}
	
	public Connection createConnection() throws SQLException
	{
		return getConnection();
	}
	
	static Map<String, Integer> readCatalog(DimCode dcode)
	{
		return DatabaseBridge.readCatalog(dcode, DbTarget.internal);
	}
	
	static void limitDeleteOld(String daycode, AggType atype)
	{
		limitDeleteOld(daycode, atype, new SimpleMail("gimp"));	
		
	}
	
	
	static List<String> getSmartWhereDateClause(String daycode)
	{
		List<String> rangelist = Util.vector();
		rangelist.add(Util.sprintf("'%s'", TimeUtil.dayBefore(daycode)));
		rangelist.add(Util.sprintf("'%s'", daycode));
		rangelist.add(Util.sprintf("'%s'", TimeUtil.dayAfter(daycode)));
		
		List<String> whlist = Util.vector();
		whlist.add(Util.sprintf(" ENTRY_DATE = '%s' ", daycode));
		whlist.add(Util.sprintf(" ID_DATE in (%s) ", Util.join(rangelist, ",")));
		return whlist;
	}
	
	// To turn OFF batching, just set DEL_BATCH to Integer.MAX_VALUE
	static int limitDeleteOld(String daycode, AggType atype, SimpleMail logmail)
	{
		double startup = Util.curtime();
		int totaldel = 0;
		
		try { 
			Connection conn = getConnection();
			
			while(true)
			{
				List<String> wh_list = getSmartWhereDateClause(daycode);
				
				//Util.pf("Going to delete data for campaign %s\n", onecamp);
				String sql = Util.sprintf("DELETE FROM %s WHERE %s LIMIT %d", 
					getAggTableName(atype), Util.join(wh_list, " AND "), DEL_BATCH);
				
				Util.pf("Deletion sql is %s\n", sql);
								
				double onestart = Util.curtime();
				int delrows = DbUtil.execSqlUpdate(sql, conn);
				totaldel += delrows;
				double tooktime = (Util.curtime()-onestart)/1000;
				
				double totaltime = (Util.curtime() - startup)/1000;
				double delrate = totaldel / totaltime;
				
				logmail.pf("Deleted %d rows in %.03f seconds, %d total, total time %.03f, avg rows/sec %.03f\n",
					delrows, tooktime, totaldel, totaltime, delrate);
				
				if(delrows < DEL_BATCH)
					{ break; }
			}	
			
			conn.close();
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
		
		return totaldel;
	}
		
	public static String getAggTableName(AggType atype)
	{
		return DatabaseBridge.getAggTableName(DbTarget.internal, atype);
	}
	
	public static List<String> getColumnNames(AggType atype)
	{
		return DatabaseBridge.getTableColNames(atype, DbTarget.internal);
	}
	
	
}
