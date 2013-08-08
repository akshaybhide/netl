package com.adnetik.bm_etl;

import java.util.*;
import java.sql.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*; 
import com.adnetik.shared.BidLogEntry.*; 
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

/**  
 * DatabaseBridge
 */
public class DatabaseTest 
{ 
	public enum DataSource { ad_general_all, v__ad_general_all, v__ad_general_1, v__ad_general_2, v__ad_general_3 };
	
	String outputData;
	String queryId;
	DataSource dSource;
	int campId;
	
	DatabaseTest(String qid, int campid, DataSource ds)
	{
		queryId = qid;
		dSource = ds;
		outputData = getOutputPath(qid, campid, dSource);
		campId = campid;
	}
	
	void query2dsv() throws Exception
	{
		List<String> qlist = FileUtils.readFileLines(getQueryPath(queryId));
		String query = Util.join(qlist, "  ");
		Util.massert(query.indexOf("XX_ID_XX") > -1);
		Util.massert(query.indexOf("XX_DATA_XX") > -1);
		query = query.replace("XX_ID_XX", ""+campId);
		query = query.replace("XX_DATA_XX", ""+dSource);
		Util.pf("Query is %s", query);
		// Util.pf("\nPath is %s, qlist is %s, query is %s", getQueryPath(queryId), qlist, query);
		query2dsv(query, "\t");
	}
	
	void query2dsv(String sqlquery, String delimiter) throws Exception
	{
		double startup = Util.curtime();
		int lcount = 0;
		
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			PreparedStatement pstmt = conn.prepareStatement(sqlquery);
			
			ResultSet rset = pstmt.executeQuery();
			ResultSetMetaData rsmd = rset.getMetaData();
			
			PrintWriter pwrite = new PrintWriter(outputData);
			
			while(rset.next())
			{
				List<String> flist = Util.vector();
				for(int c = 1; c <= rsmd.getColumnCount(); c++) {
					
					String fval = rset.getString(c);
					
					flist.add((fval == null) ? "null" : fval);
				}
				
				// Util.pf("\nFlist is %s", flist);
				
				pwrite.write(Util.sprintf("%s\n", Util.join(flist, delimiter)));
				lcount++;
				
				if((lcount % 50000) == 0)
				{ 
					Util.pf("\nFinished for lcount=%d", lcount); 
				}	
			}
			pwrite.close();
			conn.close();
			
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);		
		}
		
		Util.pf("Grabbed %d rows, took %.03f secs\n", lcount, (Util.curtime() - startup)/1000);
	}
	 
	
	public static void main(String[] args) throws Exception
	{
		String targquery = args[0];
		int campid = Integer.valueOf(args[1]);
		DataSource dsource = DataSource.valueOf(args[2]);
		runForQueryId(targquery, campid, dsource);
		
		//DatabaseTest dbtest = new DatabaseTest("dbout.test.csv");
		
		//String sqltest = "select id_date,id_campaign from ad_general where id_campaign = 988";
		// String sqltest = "select id_date,id_campaign, final_cost_usd from v__ad_general_all where id_campaign = 988";
		// dbtest.query2dsv(sqltest, "\t");
		//Util.pf("\nSQL query is %s", sqltest);
	}
	
	static String getQueryPath(String qid)
	{
		return Util.sprintf("q__%s.sql", qid);
	}
	
	static String getOutputPath(String qid, int campid, DataSource dsource)
	{
		return Util.sprintf("res__%s__%d__%s.tsv", qid, campid, dsource);
	}	
	
	static boolean runForQueryId(String qid, int campid, DataSource dsource) throws Exception
	{
		String qpath = getQueryPath(qid);	
		File probe = new File(qpath);
		if(!probe.exists())
		{ 
			Util.pf("\nFound no query id %d", qid);
			return false; 
		}
		
		Util.pf("Running for query %s\n", qid);
		DatabaseTest dbtest = new DatabaseTest(qid, campid, dsource);
		dbtest.query2dsv();
		
		return true;		
	}
}
