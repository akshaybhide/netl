
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.*;

public class ExelDbUpdate2
{
	
	public static void main(String[] args) throws Exception
	{
		Util.pf("\nRunning ExelDbUpdate2...");
		
		createTable();
		testUpload();
		
		//Util.pf("\nUpdated DB, took %.03f secs", (endtime-curtime)/1000);
	}
	
	private static Connection myConnection() throws SQLException
	{
		return DbUtil.getDbConnection("174.140.150.57", "dcb_ex2");		
	}
	
	public static void dropIfExists() throws Exception
	{
		Connection conn = myConnection();
		String sql = Util.sprintf("DROP TABLE IF EXISTS ex_data");
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();
		conn.close();		
	}
	
	public static void createTable() throws Exception
	{
		dropIfExists();

		Connection conn = myConnection();
		{
			String sql = "CREATE TABLE ex_data (wtpid varchar(40) PRIMARY KEY, seg_info varchar(400), tstamp int(64), country varchar(4))";
			sql += " PARTITION BY HASH(wtpid) PARTITIONS 256 ";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
		}
		
		{
			String sql = "CREATE INDEX  WtpIndex ON ex_data (wtpid) ";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();			
		}
		
		conn.close();		
	}	
	
	
	
	public static void testUpload() throws Exception
	{
		int lcount = 0;
		double startup = Util.curtime();
		double prvtime = Util.curtime();
		
		try {
			Connection conn = DbUtil.getDbConnection("174.140.150.57", "dcb_ex2");	
			
			Scanner sc = new Scanner(new File("ex_head_100k.txt"));
			
			String sql = "INSERT INTO ex_data (wtpid, seg_info, tstamp, country) VALUES (?, ?, ?, ?) ";
			sql += " ON DUPLICATE KEY UPDATE seg_info = ?, tstamp = ?, country = ? ";
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			
			while(sc.hasNextLine())
			{
				String oneline = sc.nextLine().trim();
				
				if(oneline.indexOf("TIMESTAMP") > -1)
					{ continue; }
				
				
				String[] toks = oneline.split("\t");
				
				long tstamp = Long.valueOf(toks[0]);
				String cty = toks[1];
				cty = cty.length() > 2 ? "" : cty;
				
				String wtpid = toks[2];
				String seg_info = toks[3];
				
				seg_info = seg_info.length() > 400 ? seg_info.substring(0, 400) : seg_info;
				
				pstmt.setString(1, wtpid);
				pstmt.setString(2, seg_info);
				pstmt.setString(5, seg_info);
				pstmt.setLong(3, tstamp);
				pstmt.setLong(6, tstamp);
				pstmt.setString(4, cty);
				pstmt.setString(7, cty);
				
				pstmt.executeUpdate();
				
				lcount++;
				
				if((lcount % 1000) == 0)
				{ 
					double endtime = Util.curtime();
					Util.pf("\nFinished update for line %d, time is %.03f", lcount, (endtime-prvtime)/1000);
					prvtime = endtime;
				}
			}
			
			conn.close();
			
			
		} catch (Exception ex) {
			
			throw new RuntimeException(ex);
		}
		
		//String sql = Util.sprintf("LOAD DATA LOCAL INFILE 'ex_head.txt' INTO TABLE ex_data ( tstamp, country, wtpid, seg_info ) ");
		// PreparedStatement pstmt = conn.prepareStatement(sql);
		// pstmt.executeUpdate();
		
	}
	
	

}
