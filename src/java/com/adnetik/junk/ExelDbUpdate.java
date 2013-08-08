
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.*;

public class ExelDbUpdate 
{
	public static final long MAX_HEX_PREF = Long.valueOf("ffffffff", 16);

	private static TreeMap<String, Integer> _PART_MAP;
	
	
	public static void main(String[] args) throws Exception
	{
		Util.pf("\nHere we are again...");
		
		createTable();
		testUpload();
		
		double curtime = Util.curtime();
		pullToMaxTable();
		updateMaxTime();
		deleteOldEntries();
		double endtime = Util.curtime();
		
		Util.pf("\nUpdated DB, took %.03f secs", (endtime-curtime)/1000);
	}
	
	private static Connection myConnection() throws SQLException
	{
		return DbUtil.getDbConnection("174.140.150.57", "dcb_ex");		
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
			String sql = "CREATE TABLE ex_data (wtpid varchar(40) , seg_info varchar(400), tstamp int(64), maxtime int(64), country varchar(4), autoid int(128) primary key AUTO_INCREMENT)";
			// sql += " PARTITION BY HASH(wtpid) PARTITIONS 256 ";
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
	

	public static void pullToMaxTable() throws Exception
	{
		Connection conn = myConnection();
		int[] delins = new int[2];
		double curtime = Util.curtime();
		
		{
			String delsql = "DELETE FROM recent_ex";
			PreparedStatement pstmt = conn.prepareStatement(delsql);
			delins[0] = pstmt.executeUpdate();			
		}
				
		{
			String delsql = "INSERT INTO recent_ex SELECT max(tstamp), wtpid FROM ex_data GROUP BY wtpid";
			PreparedStatement pstmt = conn.prepareStatement(delsql);
			delins[1] = pstmt.executeUpdate();			
		}
		
		conn.close();
		double endtime = Util.curtime();

		Util.pf("\nFinished updating max table, deleted %d old rows, inserted %d new ones, took %.03f", 
				delins[0], delins[1], (endtime-curtime)/1000);
	}
	
	
	public static void updateMaxTime() throws Exception
	{
		Connection conn = myConnection();
		String joinsql = "UPDATE ex_data INNER JOIN recent_ex ON ex_data.wtpid = recent_ex.wtpid SET ex_data.maxtime = recent_ex.maxtime";
		PreparedStatement pstmt = conn.prepareStatement(joinsql);
		int updated = pstmt.executeUpdate();		
		conn.close();
		
		Util.pf("\nUpdated maxtime for %d rows", updated);
	}	
	
	public static void deleteOldEntries() throws Exception
	{
		Connection conn = myConnection();
		String joinsql = "DELETE FROM ex_data WHERE tstamp < maxtime";
		PreparedStatement pstmt = conn.prepareStatement(joinsql);
		int deleted = pstmt.executeUpdate();		
		conn.close();
		
		Util.pf("\nDeleted %d old entries", deleted);
	}		
	
	
	public static void testUpload() throws Exception
	{
		Connection conn = DbUtil.getDbConnection("174.140.150.57", "dcb_ex");	
		String sql = Util.sprintf("LOAD DATA LOCAL INFILE 'head_ex_A.tsv' INTO TABLE ex_data ( tstamp, country, wtpid, seg_info ) ");
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();
		
		conn.close();
	}
	
	

}
