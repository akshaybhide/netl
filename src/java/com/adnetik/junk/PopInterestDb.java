
package com.adnetik.data_management;

import java.io.*;
import java.util.*;
import java.sql.*;


import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

public class PopInterestDb extends Configured implements Tool
{	
	public static final String TABLE_NAME = "pixel_wtp";
	public static String TEMP_FILE = "pop_temp_db.txt";
	
	int lineCount = 0;
	String dayCode = null;
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
		
	}

	
	public int run(String[] args) throws Exception
	{
		//createTable();

		dayCode = args[0];
		
		//Scanner sc = new Scanner(System.in);		
		copyInputToTemp();
		Util.pf("\nFinished downloading input, found %d lines", lineCount);
		loadFromFile();

		(new File(TEMP_FILE)).delete();
		
		return -1;
	}
	
	void copyInputToTemp() throws Exception
	{
		Scanner sc = new Scanner(System.in);		
		
		PrintWriter pwrite = new PrintWriter(new File("pop_temp_data.txt"));
		
		while(sc.hasNextLine())
		{
			String s = sc.nextLine();
			String[] pixwtp_count = s.split("\t");
			String[] wtp_pix = pixwtp_count[0].split(Util.DUMB_SEP);
			
			WtpId shortform = null;
			
			try { shortform = new WtpId(wtp_pix[0]); }
			catch (Exception ex) { Util.pf("\nBad WTPID: %s", wtp_pix[0]);	}

			if(shortform != null)
			{
				pwrite.write(Util.sprintf("%s\t%s\t%d\n", wtp_pix[1], shortform.toString(), Util.dayCode2Int(dayCode)));
				lineCount++;
			}
		}
		
		pwrite.close();
	}
	
	public int loadFromFile() throws SQLException
	{
		File infile = new File("pop_temp_data.txt");
		
		String sql = Util.sprintf("LOAD DATA LOCAL INFILE '%s' INTO TABLE %s ( pixid, wtpid, dateid )",
			infile.getAbsolutePath(), TABLE_NAME);
		
		//Util.pf("\nSQL statement is %s", sql);
		double startup = System.currentTimeMillis();
		Connection conn = DbUtil.getDbConnection("interest_db");
		int numadded = DbUtil.executeUpdate(conn, sql);	
		double endtime = System.currentTimeMillis();
		
		Util.pf("\nFound %d lines, added %d rows, took %.03f secs", lineCount, numadded, (endtime-startup)/1000);
		
		return -1;
	}	
	
	
	void dropIfExists() throws SQLException
	{
		String sql = Util.sprintf("drop table if exists %s", TABLE_NAME);
		Connection conn = DbUtil.getDbConnection("interest_db");
		DbUtil.executeUpdate(conn, sql);
	}	
	
	void createTable() throws SQLException
	{
		dropIfExists();
		
		Connection conn = DbUtil.getDbConnection("interest_db");
		
		{
			String sql = Util.sprintf("create table %s (pixid int(32) NOT NULL, wtpid char(36) NOT NULL, dateid int NOT NULL)", TABLE_NAME);
			DbUtil.executeUpdate(conn, sql);
		}
		
		{
			String sql = Util.sprintf("create index pix_index on %s (pixid)", TABLE_NAME);
			DbUtil.executeUpdate(conn, sql);
		}		
		
	}
}
