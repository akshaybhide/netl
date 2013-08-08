package com.digilant.mobile;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import javax.sql.rowset.CachedRowSet;
import com.adnetik.shared.DbUtil;
import com.adnetik.shared.Util;

public class DBConnection {
	private Connection _conn;
	public DBConnection(){
	}
	public static Connection getNZConnection(String machine, String db) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		//String jdbcurl = Util.sprintf("jdbc:mysql://%s/%s", "thorin-internal.adnetik.com", "fastetl");
		Class.forName("org.netezza.Driver").newInstance();
		Connection conn = DriverManager.getConnection( "jdbc:netezza://66.117.49.50/"+db, "armita", "data_101?" );
		return conn;				
	}
	public static Connection renewConnection(String db_type, String machine, String db) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		if(db_type.toLowerCase().contains("q"))
			return DBConnection.getConnection(machine, db);
		return DBConnection.getNZConnection(machine, db);
		
	}
	public static Connection getConnection(String machine, String db) throws SQLException
	{
		//DbUtil.doClassInit();
		DbUtil.doMysqlClassInit();
		//String jdbcurl = Util.sprintf("jdbc:mysql://%s/%s", "thorin-internal.adnetik.com", "fastetl");
		String jdbcurl = Util.sprintf("jdbc:mysql://%s/%s", machine, db);

		// Util.pf("JDBC url is %s\n", jdbcurl);
		
		//String jdbcurl = Util.sprintf("jdbc:mysql://66.117.49.93/%s", dbname);
		
		String uname = "armita";
		String pword;
		pword = "data_101?";
		Connection conn = DriverManager.getConnection(jdbcurl, uname, pword);

		return conn;				
		
	}
	public  static ArrayList<String> lookupColumns(String machine, String db, String table) throws SQLException{
		
		String query = Util.sprintf("DESCRIBE %s", table);
		
			CachedRowSet rs = runQuery(machine, db, query);
			ArrayList<String> collist = new ArrayList<String>();
			while(rs.next()){
				collist.add(rs.getString(1));
			}
			return collist;
			
	}
	public  static ArrayList<String> lookupColumns(String dbtype, Connection conn, String table) throws SQLException{
		String query="";
		if(dbtype.toLowerCase().contains("q")) 
				query = Util.sprintf("DESCRIBE %s", table);
		else 
			query = Util.sprintf("select  COLUMN_NAME from _V_ODBC_COLUMNS1 where table_name='%s' order by ORDINAL_POSITION", table.toUpperCase());
		
			CachedRowSet rs = runBatchQuery(conn, query);
			ArrayList<String> collist = new ArrayList<String>();
			while(rs.next()){
				collist.add(rs.getString(1).toLowerCase());
			}
			return collist;
			
	}

	public  static LinkedHashMap<String, String> lookupColumnsAndTypes(String machine, String db, String table) throws SQLException{
		
		String query = Util.sprintf("DESCRIBE %s", table);
		
			CachedRowSet rs = runQuery(machine, db, query);
			LinkedHashMap<String, String> colname_and_type = new LinkedHashMap<String, String>();
			while(rs.next()){
				colname_and_type.put(rs.getString(1), rs.getString(2));
			}
			return colname_and_type;
			
	}

	public static CachedRowSet runQuery(String machine, String db, String query) throws SQLException{
		
		
			CachedRowSet crs = CachedRowSetFactory.getCachedRowSet();;
			Connection conn = getConnection(machine, db);
			Statement statement = conn.createStatement();
			ResultSet  rs = null;
			if(query.toLowerCase().startsWith("select")|| query.toLowerCase().startsWith("describe")){
				rs = statement.executeQuery(query);
				crs.populate(rs);
				rs.close();
			}
			else 
				statement.executeUpdate(query);
				
			conn.close();
			return crs;
			
	}
	public static CachedRowSet runBatchQuery(Connection conn, String query) throws SQLException{
		
		return runBatchQuery(conn, query, new ArrayList<Integer>());
	}

	public static CachedRowSet runBatchQuery(Connection conn, String query, ArrayList<Integer> cnt) throws SQLException{
		
			CachedRowSet crs = CachedRowSetFactory.getCachedRowSet();
			Statement statement = conn.createStatement();
			ResultSet  rs = null;
			if(query.toLowerCase().startsWith("select")|| query.toLowerCase().startsWith("describe")){
				rs = statement.executeQuery(query);
				crs.populate(rs);
				rs.close();
			}
			else {
				 	int i = statement.executeUpdate(query);
				 	if(cnt.size()==0)
				 		cnt.add(i);
				 	else
				 		cnt.set(0, i + cnt.get(0));
				 	
			}
				
			return crs;
			
	}

}
