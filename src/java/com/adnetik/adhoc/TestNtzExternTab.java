package com.adnetik.adhoc;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.DbUtil.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

public class TestNtzExternTab
{
	
	public static void main(String[] args)
	{
		Util.pf("Going to test NTZ external table upload speed.");
		
		TestNtzExternTab tnet = new TestNtzExternTab();
		tnet.uploadExtTable("extern_dim_test");
		
	}
	
	private String getExtTableName(String tabname)
	{
		return Util.sprintf("%s_extcopy", tabname);	
	}
	
	private void uploadExtTable(String tabname)
	{
		Connection conn = null;
		
		try {
			conn = NZConnSource.getNetezzaConn("FASTETL").createConnection();		
			
			{
				PreparedStatement pstmt = conn.prepareStatement(Util.sprintf("delete from %s", tabname));
				int delrows = pstmt.executeUpdate();
				Util.pf("Deleted %d old rows\n", delrows);
			} 
			
			{
				String sql = Util.sprintf("insert into %s select * from %s", tabname, getExtTableName(tabname));
				PreparedStatement pstmt = conn.prepareStatement(sql);
				int inrows = pstmt.executeUpdate();	
				Util.pf("Inserted %d new rows\n", inrows);

			}
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);	
			
		} finally {
			
			if (conn != null)
			{
				try { conn.close(); }
				catch (SQLException sqlex) {}
			}
		}
	}
}
