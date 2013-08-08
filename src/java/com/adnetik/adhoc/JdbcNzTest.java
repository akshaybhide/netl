
package com.adnetik.adhoc;

import java.sql.*;
import java.util.*;


import com.adnetik.shared.*;
import com.adnetik.shared.DbUtil.*;

public class JdbcNzTest 
{
	public static void main2(String[] args)  throws Exception
	{
		String sql = "SELECT entry_date, id_campaign FROM cbicca LIMIT 20";
		
		ConnectionSource csource = NZConnSource.getNetezzaConn("fastetl");
		
		List<Pair<java.sql.Date, Integer>> pairlist = DbUtil.execSqlQueryPair(sql, csource);
		
		for(Pair<java.sql.Date, Integer> onepair : pairlist)
		{
			Util.pf("Pair is %s\n", onepair);	
			
		}
		
		/*
		String server = TestNzUpload.NETEZZA_HOST_ADDR;
		
		
		// String port = "5480";
		String dbName = "FASTETL";
		String url = "jdbc:netezza://" + server + "/" + dbName ;
		String user = "burfoot";
		String pwd = "data_101?";

		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		try {
			Class.forName("org.netezza.Driver");
			System.out.println(" Connecting ... ");
			conn = DriverManager.getConnection(url, user, pwd);
			// System.out.println(" Connected "+conn);
			
			String sql = "select domain from imp_test_1 limit 10";
			st = conn.createStatement();
			rs = st.executeQuery(sql);
			
			while(rs.next())
			{
				Util.pf("Result is %s\n", rs.getString(1));	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if( rs != null) 
					rs.close();
				if( st!= null)
					st.close();
				if( conn != null)
					conn.close();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
		*/
	}
	
	public static void main(String[] args)
	{
		String sql = "INSERT INTO daily_exchange_rates (id_date, curr_code, one_usd_buys) VALUES ('2013-01-02', 'BTC', 4.5)";
		ConnectionSource csource = NZConnSource.getNetezzaConn("fastetl");
		
		DbUtil.execSqlUpdate(sql, csource);
		
		/*
		String sql = "SELECT id_date, one_usd_buys FROM daily_exchange_rates WHERE curr_code = 'GBP' LIMIT 20";
		
		
		List<Pair<java.sql.Date, Integer>> pairlist = DbUtil.execSqlQueryPair(sql, csource);
		
		for(Pair<java.sql.Date, Integer> onepair : pairlist)
		{
			Util.pf("Pair is %s\n", onepair);	
			
		}		
		*/
		
	}
}
