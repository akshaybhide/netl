
package com.adnetik.shared;

import java.sql.*;
import java.util.*;

import com.adnetik.shared.*;

public class DbConnect
{
	
	private static boolean classInit = false;
	
	public static synchronized void doClassInit()
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
	
	public static Connection getLocalConnection(String dbname) throws SQLException
	{
		doClassInit();
		
		String jdbcurl = "jdbc:mysql://66.117.49.93/test";
		
		String uname = "burfoot";
		String pword = "dcb_data";
		
		//String jdbcurl = Util.sprintf("jdbc:mysql://localhost/%s?user=root&password=GaneshSQL", dbname);
		
		Connection conn = DriverManager.getConnection(jdbcurl, uname, pword);

		return conn;
	}
	
	public static Connection getUceConnection() throws Exception
	{ 
		
		doClassInit();
		
		String ipaddress = "66.117.49.93";
		
		String jdbcurl = Util.sprintf("jdbc:mysql://%s/uce", ipaddress);

		//String jdbcurl = Util.sprintf("jdbc:mysql://66.117.49.93/%s", dbname);
		
		String uname = "huimin";
		String pword = "Adnetik100NW502";
		
		//String jdbcurl = Util.sprintf("jdbc:mysql://localhost/%s?user=root&password=GaneshSQL", dbname);
		
		Connection conn = DriverManager.getConnection(jdbcurl, uname, pword);

		Util.sprintf("\nGot a connection");
		
		return conn;		
	}	
	
	public static Connection getLocalConnection2(String dbname) throws Exception
	{ 
		
		doClassInit();
		
		String jdbcurl = Util.sprintf("jdbc:mysql://localhost/%s", dbname);

		//String jdbcurl = Util.sprintf("jdbc:mysql://66.117.49.93/%s", dbname);
		
		String uname = "root";
		String pword = "GaneshSQL";
		
		//String jdbcurl = Util.sprintf("jdbc:mysql://localhost/%s?user=root&password=GaneshSQL", dbname);
		
		Connection conn = DriverManager.getConnection(jdbcurl, uname, pword);

		return conn;		
	}
	
	static void doTest() throws Exception
	{
		String insertSql = "INSERT INTO maximine VALUES ( ?, ?, ? )";
		Connection conn = getLocalConnection("test");

		for(int i = 200; i < 250; i++)
		{
			PreparedStatement pstmt = conn.prepareStatement(insertSql);			
			pstmt.setInt(1, i);
			pstmt.setInt(2, i+10);
			pstmt.setString(3, "testvalue" + i);
			pstmt.executeUpdate(); 			
			Util.pf("\nUpdate for item %d", i);
		}
	}
	
	/*
	static void doDropTables() throws Exception
	{
		String showtab = "SHOW FULL TABLES";
		Connection conn = getLocalConnection3("bm_etl");

		PreparedStatement pstmt = conn.prepareStatement(showtab);			
		ResultSet rset = pstmt.executeQuery(); 
		
		//Util.pf("\nUpdate for item %d", i);	
		
		while(rset.next())
		{
			String tablename = rset.getString(1);
			boolean isview = rset.getString(2).indexOf("VIEW") > -1;
			
			//Util.pf("\nString is %s", tablename);
			
			if(tablename.indexOf("ad_agg_general") > -1)
			{
				dropSpecTable(conn, tablename, isview);	
			}
		}
	}	
	*/
	
	static void dropSpecTable(Connection conn, String tabname, boolean view) throws SQLException
	{
		String dropsql = Util.sprintf("drop %s %s", (view ? "view" : "table"),  tabname);
		PreparedStatement pstmt = conn.prepareStatement(dropsql);			
		int num = pstmt.executeUpdate(); 
		Util.pf("\nDropped table %s, affected rows = %d", tabname, num);
	}
	
	
	public static void main(String[] args) throws Exception 
	{
		
		Util.pf("\nSTarting");
		
		//getUceConnection();
		
		Util.pf("\nGot a connection");
	}
}
