
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.slicerep.SliUtil.*;

public class CreativeUpdater
{	
	public static final int APPNEXUS_ID = 5;
	
	public enum CType { REG, APPN };
	
	public static void main(String[] args) throws SQLException
	{
		//rebuildLookups(true);
		//rebuildLookups(false);
		
		showUpdateSql();
	}
	
	private static void showUpdateSql()
	{
		String sql = Util.sprintf("UPDATE %s SET FAST.id_creative = %s WHERE FAST.id_creative = -1",  getTableJoinList(), getIfSql());
		
		Util.pf("SQL is \n%s\n", sql);
		
	}	
	
	private static String getIfSql()
	{
		return Util.sprintf(" IF(FAST.id_exchange = %d, APPN.ctv_id, REG.ctv_id) ", APPNEXUS_ID);
		
	}
	
	private static void showSelectSql()
	{
		String sql = Util.sprintf("SELECT REG.id, APPN.id FROM %s LIMIT 10",  getTableJoinList());
		
		Util.pf("SQL is \n%s\n", sql);
		
	}
	
	private static String getTableJoinList()
	{
		return Util.sprintf(" fast_general FAST %s %s ", getJoinClause(CType.REG), getJoinClause(CType.APPN));
		
	}
	
	private static String getJoinClause(CType ctype)
	{
		return Util.sprintf(" LEFT JOIN %s %s ON FAST.id_assignment = %s.id", 
			getLookupTableName(ctype == CType.REG), ctype, ctype);
		
	}
	
	private static String getLookupTableName(boolean isreg)
	{
		return "__creative_lookup__" + (isreg ? "reg" : "appnexus");
	}

	private static void rebuildLookups(boolean isreg) 
	{
		String dsttable = "__creative_lookup__" + (isreg ? "reg" : "appnexus");
		String idcolumn = (isreg ? "id" : "appnexus_id");
		
		try {
			Connection conn = SliDatabase.getConnection();
			
			{
				String delsql = Util.sprintf("DELETE FROM %s", dsttable);	
				int delrows = DbUtil.execSqlUpdate(delsql, conn);
				Util.pf("Deleted %d old rows from %s\n", delrows, dsttable);
			}
			
			{
				
				String inssql = Util.sprintf("INSERT INTO %s SELECT distinct(%s), creative_id FROM adnetik.assignment WHERE %s IS NOT NULL",
					dsttable, idcolumn, idcolumn);
				int insrows = DbUtil.execSqlUpdate(inssql, conn);
				Util.pf("Inserted %d new rows into %s\n", insrows, dsttable);
			}
			
			conn.close();

		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);	
		}
		
	}
}
