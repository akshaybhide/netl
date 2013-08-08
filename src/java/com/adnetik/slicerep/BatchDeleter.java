
package com.adnetik.slicerep;

import java.util.*;
import java.io.*;
import java.sql.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.BidLogEntry.*;


public class BatchDeleter
{
	SimpleMail _logMail;
	
	private String _dayCode;
	
	private DbTarget _dbTarg = DbTarget.internal;
	
	private int _batchTarget = 1000000; // one million
	
	public static void main(String[] args)
	{
		Set<String> dayset = Util.treeset();
		dayset.add("2013-05-28");
		dayset.add("2013-05-29");
		dayset.add("2013-05-30");
		
		if(Util.checkOkay(Util.sprintf("Okay to delete for dayset: %s", dayset.toString())))
		{
			for(String onedate : dayset)
			{
				BatchDeleter bdel = new BatchDeleter(onedate);
				bdel.doDelete(true);
			}
		}
		
	}
	
	public BatchDeleter(String dc)
	{
		this(dc, new SimpleMail("BatchDeleteReport for " + dc));
	}
	
	public BatchDeleter(String dc, SimpleMail lmail)
	{
		Util.massert(TimeUtil.checkDayCode(dc), "Invalid daycode %s", dc);
		_dayCode = dc;

		_logMail = lmail;
	}
	
	void setBatchTarget(int bt)
	{
		_batchTarget = bt;	
	}
	
	void setDbTarget(DbTarget dbt)
	{
		_dbTarg = dbt;	
	}
	
	void doDelete(boolean dosend)
	{
		List<String> whlist = SliDatabase.getSmartWhereDateClause(_dayCode);
		
		for(AggType onetype : AggType.values())
		{
			for(int i = 0; i < 1000; i++)
			{			
				try { 
					String sql = Util.sprintf("DELETE FROM %s WHERE %s LIMIT %d", 
						SliDatabase.getAggTableName(onetype), Util.join(whlist, " AND "), _batchTarget);
					
					Connection conn = (new DatabaseBridge(_dbTarg)).createConnection();	
					
					String opcode = Util.sprintf("DELETE RUN %d DAYCODE %s AGGTYPE %s", i, _dayCode, onetype);
					
					int delrows = DbUtil.execWithTime(sql, opcode, conn, _logMail);
					
					if(delrows < _batchTarget)
						{ break; }
					
					conn.close();
					
				} catch (SQLException sqlex) {
					
					sqlex.printStackTrace();
					_logMail.pf("Got SQL Exception %s, retrying", sqlex.getMessage());
				}
			}
		}
		
		if(dosend)
			{ _logMail.send2admin(); }
	}
}
