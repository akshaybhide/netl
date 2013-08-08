
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

public class RetargetingQuery extends AbstractMapper.LineFilter
{
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
		
		if(ble == null)
			{ return null; }
		
		String campId = ble.getField("campaign_id").trim();
		String lineId = ble.getField("line_item_id").trim();
		String lineType = ble.getField("line_item_type").trim();
		
		if(lineType.length() == 0)
		{ 
			lineType = "none";
		}
	
		//Util.pf("\nLine id is %s, type is %s", lineId, lineType);		
		return new String[] { lineId, Util.sprintf("%s\t%s", campId, lineType) };
	}
	
	public static class InsertReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		Connection conn = null;
		String insertSql = "INSERT INTO line_info VALUES ( ?, ?, ?)";
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{			
			Set<String> typeset = Util.treeset();
			
			while(values.hasNext())
			{
				String nextval = values.next().toString();
				typeset.add(nextval);
			}
			
			int lineId = Integer.valueOf(key.toString());
						
			//collector.collect(key, new LongWritable(maxVal));
			//collector.collect(key, new Text("" + typeset.size()));
			
			try { 
				if(conn == null)
				{
					//conn = DbConnect.getConnection();
				}
				
				for(String onetype : typeset)
				{
					String[] toks = onetype.split("\t");
					
					PreparedStatement pstmt = conn.prepareStatement(insertSql);
					pstmt.setInt(1, lineId);
					pstmt.setInt(2, Integer.valueOf(toks[0]));	
					pstmt.setString(3, toks[1]);
					pstmt.executeUpdate(); 
				}
				
				collector.collect(key, new Text("" + typeset.size()));		
				
			} catch (SQLException sqlex) {
				
				collector.collect(new Text("SQLException"), new Text(sqlex.getMessage()));
				//Util.pf("\nHit SQL exception: %s", sqlex.getMessage());
				//throw new RuntimeException(sqlex);	
			}		
		}
	}	

	// Dan Burfoot was here
}
