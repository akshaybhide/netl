
package com.adnetik.shared;

import java.sql.*;
import java.util.*;
import java.io.*;


import com.adnetik.shared.*;

public class Maxmine
{
	public static int MAX_ORG_NAME_LEN = 300;
	
	private static boolean classInit = false;
	
	private static Connection _CONN;
	
	
	private static Connection getLocalConn()
	{
		if(_CONN == null)
		{
			try { _CONN = getConnection(); }
			catch (SQLException sqlex) 
			{ throw new RuntimeException(sqlex);	}
		}
		
		return _CONN;
	}
	
	
	public static Connection getConnection() throws SQLException
	{
		doClassInit();
		
		String jdbcurl = "jdbc:mysql://localhost/maxmine?user=root&password=GaneshSQL";
		
		Connection conn = DriverManager.getConnection(jdbcurl);

		return conn;
	}	
	private static void doClassInit()
	{
		if(classInit)
			{ return ; }
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			classInit = true;
		} catch (Exception ex )  {
			
			throw new RuntimeException(ex);	
		}
	}
	
	private static void dropTable() throws SQLException
	{
		try {
			Connection conn = getConnection();
			String dropSql = "DROP TABLE ip_info";
			PreparedStatement pstmt = conn.prepareStatement(dropSql);	
			int rows = pstmt.executeUpdate();
			Util.pf("\nTable dropped, %d rows", rows);
		} catch (SQLException notable) {
			// This is a dumb way to check, but hell
			Util.pf("Table not present");			
		}
	}

	static Scanner getMaxmineScanner() throws IOException
	{
		return Util.getGzipScanner("/home/burfoot/GeoIPOrg.csv.gz", "UTF-8");
	}
	
	static void showMaxmineData() throws IOException
	{
		Scanner sc = getMaxmineScanner();
		
		while(sc.hasNextLine())
		{
			Util.pf("\nLine is %s", sc.nextLine());
		}
		
		sc.close();
	}
	
	
	
	static void bigDataInsert() throws Exception
	{
		Scanner sc = getMaxmineScanner();
		int rowcount = 0;
		Connection conn = getConnection();
		
		while(sc.hasNextLine())
		{
			String oneline = sc.nextLine();
			String[] toks = oneline.split(",");
			
			int lowend = Integer.valueOf(toks[0]);
			int hghend = Integer.valueOf(toks[1]);

			insertRow(lowend, hghend, toks[2], conn);
			
			rowcount++;
			
			if((rowcount % 1000) == 0)
			{
				Util.pf("\n-------------------------------");
				Util.pf("\nInserted row %d, orgname was %s", rowcount, toks[2]);
			}
			
		}
		
		conn.close();
		sc.close();
		Util.pf("\nInserted %d total rows", rowcount);
		
	}
	
	static void insertRow(int lowend, int hghend, String orgname, Connection conn) 
	throws Exception
	{
		String insertSql = "INSERT INTO ip_info VALUES ( ?, ?, ? )";
		
		String toins = (orgname.length() > MAX_ORG_NAME_LEN ? orgname.substring(MAX_ORG_NAME_LEN) : orgname);

		PreparedStatement pstmt = conn.prepareStatement(insertSql);			
		pstmt.setInt(1, lowend);
		pstmt.setInt(2, hghend);
		pstmt.setString(3, toins);
		pstmt.executeUpdate(); 		
		
		//Util.pf("\n\tInserted range %d-%d for %s", lowend, hghend, orgname);
	}	
	
	public static String lookupIp(String ip)
	{
		long longform = Util.ip2long(ip);
		
		Connection conn = getLocalConn();
		
		try {
			
			String querySql = Util.sprintf("SELECT * FROM ip_info WHERE lowend <= ? AND ? <= hghend");
			PreparedStatement pstmt = conn.prepareStatement(querySql);	
			
			pstmt.setLong(1, longform);
			pstmt.setLong(2, longform);
			ResultSet rset = pstmt.executeQuery();		
			
			if(rset.next())
			{
				return rset.getString(3);
			}
			
			return null;
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
		} // blah blah close
		
		
	}
	
	// setup the table
	private static void initTable() throws SQLException
	{
		Connection conn = getConnection();
		String createSql = Util.sprintf("CREATE TABLE ip_info (lowend int, hghend int, org_name varchar(%d))", MAX_ORG_NAME_LEN);
		PreparedStatement pstmt = conn.prepareStatement(createSql);	
		pstmt.executeUpdate();
		Util.pf("\nCreated table");
	}
	
	private static void testLookup()
	{
		Random jr = new Random();
		
		long toplong = 128L*256;
		toplong *= 256;
		toplong *= 256;
		
		convertTest(256);
		
		for(int i = 0; i < 10000; i++)
		{
			long big = jr.nextLong();
			if(big < 0)
				{ continue; }
			
			long testA = big % (toplong);
			String testip = Util.long2ip(testA);
			String orgname = lookupIp(testip);
			
			if(orgname != null)
				{ Util.pf("\nFound orgname %s for testip=%s", orgname, testip); }
		
		}
	}	
	
	
	
	private static void testConversion()
	{
		Random jr = new Random();
		
		long toplong = 128L*256;
		toplong *= 256;
		toplong *= 256;
		
		convertTest(256);
		
		for(int i = 0; i < 10000; i++)
		{
			long big = jr.nextLong();
			if(big < 0)
				{ continue; }
			
			long testA = big % (toplong);
			//Util.pf("\nBig=%d, testA=%d, toplong=%d", big, testA, toplong);
			convertTest(testA);
		}
	}
	
	private static void convertTest(long testA)
	{
		String quad = Util.long2ip(testA);
		long testB = Util.ip2long(quad);
		Util.massertEq(testA, testB);
		Util.pf("\nTestA=%d, quad=%s, testB=%d", testA, quad, testB);
	}
	
	
	public static void main(String[] args) throws Exception
	{
		//getConnection();
		dropTable();
		initTable();
		//bigDataInsert();
		
		// showMaxmineData();
		//lookupIp("129.42.38.1");
		//testLookup();
		Util.pf("\n\n");
	}
}
