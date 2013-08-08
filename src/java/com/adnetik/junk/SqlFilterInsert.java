
package com.adnetik.adhoc;

import java.util.*;
import java.io.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.analytics.*;
import com.adnetik.shared.Util.LogType;	

//import com.mysql.jdbc.Driver;

public class SqlFilterInsert extends AbstractMapper.LineFilter
{	
	Connection conn; 	
	String insertPriceSql = "INSERT INTO price_info VALUES ( ?, ?, ?, ?, ? )";
		
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line.toString());
		if(ble == null)
			{ return null; }
				
		int camp_id = ble.getIntField("campaign_id");
		int line_id = ble.getIntField("line_item_id");		
		String timestamp = ble.getField("date_time");
		double bid_cpm = ble.getDblField("bid");
		
		
		double price_cpm = ble.getDblField("winner_price") / 1000;
				
		Util.massert(bid_cpm >= price_cpm);
		
		try { 
			if(conn == null)
			{
				//conn = DbConnect.getConnection();
			}
			
			PreparedStatement pstmt = conn.prepareStatement(insertPriceSql);
			
			pstmt.setLong(1, -1);
			pstmt.setInt(2, line_id);
			pstmt.setInt(3, camp_id);
			
			pstmt.setDouble(4, price_cpm);
			pstmt.setDouble(5, bid_cpm);
			
			pstmt.executeUpdate(); 
			pstmt.close();
			
			
			//Util.pf("\nUpdated from domain %s", domtop[0]);
		} catch (SQLException sqlex) {
			
			Util.pf("\nSQL exception: %s", sqlex.getMessage());
			return new String[] { sqlex.getMessage(), "1" };
			//return new String[] { "SQLException", sqlex.getMessage() };
		}
		
		return new String[] { "success", "1" };
	}
}

