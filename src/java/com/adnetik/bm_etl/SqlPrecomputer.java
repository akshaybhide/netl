package com.adnetik.bm_etl;

import java.util.*;
import java.sql.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*; 
import com.adnetik.shared.BidLogEntry.*; 
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

/**
 * SqlPrecomputer
 */
public class SqlPrecomputer
{	
	public enum SqlUpdate { slow_global_c };
	
	private static final String DAYCODE_MAGIC = "XX_MAGIC_DAYCODE_XX";
	
	private String _dayCode; 
	private SimpleMail _logMail;
	
	public static void main(String[] args)
	{
		SqlPrecomputer sqlp = new SqlPrecomputer();
		sqlp.runUpdates();
	}
	
	public SqlPrecomputer()
	{
		_dayCode = TimeUtil.getYesterdayCode();
		_logMail = new SimpleMail("SQL Updater for " + _dayCode);
	}
	
	private void runUpdates()
	{
		for(SqlUpdate onejob : SqlUpdate.values())
			{ onePrecompute(onejob); }
		
		_logMail.send2admin();		
	}
	
	private void onePrecompute(SqlUpdate job)
	{
		String jobsql = readSqlData(job);
		jobsql = jobsql.replaceAll(DAYCODE_MAGIC, "'" + _dayCode + "'"); 
		
		try {
			double startup = Util.curtime();
			Connection conn = (new DatabaseBridge(DbTarget.internal)).createConnection();
			Statement stmt = conn.createStatement();
			int batchcount = 0;
			
			for(String onestat : jobsql.split(";"))
			{
				if(onestat.trim().length() == 0)
					{ continue; }
				
				// Util.pf("Adding batch command \n%s\n", onestat);	
				stmt.addBatch(onestat);
				batchcount++;
			}
			
			_logMail.pf("Running job %s, have %d SQL blocks", job, batchcount);
			
			int[] rows = stmt.executeBatch();

			// ughhh
			List<Integer> rowlist = Util.vector();
			for(int r : rows)
				{ rowlist.add(r); }
			
			double timesecs = (Util.curtime() - startup)/1000;
			_logMail.pf("Finished job %s, row updates are , rows %s, took %.03f seconds", job, Util.join(rowlist, ","), timesecs);
			
			conn.close();
			
		} catch (SQLException sqlex) {

			// _logMail.pf("SQL update FAILED!!!");			
			// _logMail.addExceptionData(sqlex);
			throw new RuntimeException(sqlex);
		}
		
		// DbUtil.execWithTime(jobsql, job.toString().toUpperCase(), new DatabaseBridge(DbTarget.internal), _logMail);		
		
	}
	
	private String readSqlData(SqlUpdate job)
	{
		String sqlrespath = Util.sprintf("%s/bm_etl/%s.sql", Util.RESOURCE_BASE, job.toString());
		InputStream resource = SqlPrecomputer.class.getResourceAsStream(sqlrespath);
		
		StringBuffer sb = new StringBuffer();
		Scanner sc = new Scanner(resource, "UTF-8");
		
		while(sc.hasNextLine())
		{
			sb.append(sc.nextLine());
			sb.append("\n");
		}
		
		return sb.toString();
	}
}
