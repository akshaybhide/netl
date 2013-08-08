/**
 * 
 */
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.shared.BidLogEntry.*;


public class TestGoogViz
{

	public static void main(String[] args) throws Exception
	{
		
		Util.pf("TestGoogViz");
		//showSomeInfo();
		
		ColChartGen ccg = new ColChartGen();
		Util.pf("%s", ccg.getDoc());
	}
	
	static void showSomeInfo()
	{
		/*
		try {
			
			
			String sql = "select id_browser, id_country from ad_general where id_campaign = 1271 and id_date = 20120125";
			
			Connection conn = DatabaseBridge.getDbConnection();
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			
			int rcount = 0;
			
			while(rset.next())
			{
				int a = rset.getInt(1);
				int b = rset.getInt(2);
				
				rcount++;
				
				if(rcount < 100)
					{ Util.pf("\nA=%d, B=%d", a, b); }
			}
			
			Util.pf("\nTotal rcount is %d", rcount);
			
			
		} catch (SQLException sqlex) {
			
			sqlex.printStackTrace();	
		}
		*/
	}
	
	public static class ColChartGen
	{
		List<Object[]> rowData = Util.vector();
		
		public ColChartGen()
		{
			rowData.add(new Object[] { "2004", 1000, 400 });
			rowData.add(new Object[] { "2004", 1000, 400 });
			
		}
		
		
		String getDataRows()
		{
			StringBuilder sb = new StringBuilder();
			sb.append("\ndata.addRows([");
			
			for(Object[] onerow : rowData)
			{
				sb.append("\n\t").append(row2str(onerow));
			}
			
			sb.append("\n]);");
			return sb.toString();
		}
		
		String row2str(Object[] onerow)
		{
			return null;
			
			
		}
		
		public String getDoc() throws IOException
		{
			StringBuilder sb = new StringBuilder();

			List<String> templines = FileUtils.readFileLines("/mnt/data/burfoot/googviz/col_chart_template.html");
			
			for(String onetemp : templines)
			{
				if(onetemp.indexOf("xxxDATA_ROWSxxx") > -1)
				{
					sb.append(getDataRows());
					
				} else {
					sb.append(onetemp).append("\n");	
				}
				
				
			}
			
			
			return sb.toString();
		}
	}
	
}
