
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

public class SqlPriceDelta extends AbstractMapper.LineFilter
{	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line.toString());
		if(ble == null)
			{ return null; }
		
		//String domain = ble.getField("domain").trim();
		
		String url = ble.getField("url").trim();
		
		if(url.length() == 0)
			{ return null; }
		
		String[] domtop = Util.getDomTopFromUrl(url);
		
		if(domtop == null) 
			{ return null; }
		
		//Util.pf("\nDomain is %s", domtop[0])
		
		return new String[] { domtop[0], "1" };

	}
	 
	public static class SqlAddReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		Connection conn = null;
		String insertSql = "INSERT INTO dumbdomain VALUES ( ?, ?)";
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{
			/*
			Set<String> x = Util.treeset();			
			
			while(values.hasNext())
			{
				x.add(values.next().toString());
			}
			
			//collector.collect(key, new LongWritable(maxVal));
			collector.collect(key, new Text("" + x.size()));

			try { 
				if(conn == null)
				{
					conn = DbConnect.getConnection();
				}
				
				PreparedStatement pstmt = conn.prepareStatement(insertSql);
				pstmt.setString(1, key.toString());
				pstmt.setInt(2, total);
				
				pstmt.executeUpdate(); 
				Util.pf("\nTotal for domain %s is %d", key.toString(), total);
			} catch (SQLException sqlex) {
				
				collector.collect(new Text("SQLException"), new Text(sqlex.getMessage()));
				//Util.pf("\nHit SQL exception: %s", sqlex.getMessage());
				//throw new RuntimeException(sqlex);	
			}
			*/
			
		}

	}	
}

