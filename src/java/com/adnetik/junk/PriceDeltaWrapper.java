
package com.adnetik.analytics;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;
import com.adnetik.analytics.CountMapWrapper.LineGraphWrapper;

// Eventually, this should just be a wrapper around some kind of database call.
public class PriceDeltaWrapper
{
	Map<String, String> optArgs = Util.treemap();
	
	Map<String, Integer> gapMap = Util.treemap();
	Map<String, Integer> prcMap = Util.treemap();
	
	Set<Integer> targLineSet = null;
	
	public static void main(String[] args) throws Exception
	{
		(new PriceDeltaWrapper()).run(args);
	}
	
	void run(String[] args) throws Exception
	{
		{
			optArgs.put("targ_id", 1887950658 + "");
			optArgs.put("outputprceps", "prcgraph.eps");
			optArgs.put("outputgapeps", "gapgraph.eps");
			optArgs.put("line_type", "none");
			Util.putClArgs(args, optArgs);
		}
		
		for(String k : optArgs.keySet())
		{
			Util.pf("\n\tOption is %s = %s", k, optArgs.get(k));	
		}
			
		generateGraph();			
		
		Util.pf("\nOperation successful\n\n");
		
	}
	
	void generateGraph() throws Exception
	{	
		genLineFilterSet();
		
		runQuery();
		
		if(gapMap.size() == 0 || prcMap.size() == 0)
		{
			throw new RuntimeException("No results found");	
		}
		
		LineGraphWrapper gapWrap = new LineGraphWrapper();
		LineGraphWrapper prcWrap = new LineGraphWrapper();
		
		
		gapWrap.dataMap = Util.treemap();
		prcWrap.dataMap = Util.treemap();
		
		for(String k : gapMap.keySet())
		{
			gapWrap.dataMap.put(Double.valueOf(k), (double) gapMap.get(k));
		}
		
		for(String k : prcMap.keySet())
		{
			prcWrap.dataMap.put(Double.valueOf(k), (double) prcMap.get(k));	
		}
		
		gapWrap.bottomLab = "Value Gap (bid - price)";
		prcWrap.bottomLab = "Price";
		
		gapWrap.leftLab = "# Impressions";
		prcWrap.leftLab = "# Impressions";
		
		gapWrap.title = Util.sprintf("Value Gap Graph for TargId=%s, LineType=%s", optArgs.get("targ_id"), optArgs.get("line_type"));
		prcWrap.title = Util.sprintf("Price Graph for TargId=%s, LineType=%s", optArgs.get("targ_id"), optArgs.get("line_type"));

		gapWrap.writeEps(optArgs.get("outputgapeps"));
		prcWrap.writeEps(optArgs.get("outputprceps"));
		
	}
	
	Map<String, Integer> runQuery() throws SQLException
	{
		double startTime = System.currentTimeMillis();
		
		int targId = Integer.valueOf(optArgs.get("targ_id"));
		Map<String, Integer> hitmap = Util.treemap();
		Connection conn = DbConnect.getConnection();
		
		String selectSql = getSelectSql();
		
		// Util.pf("\nSelect sql is %s", selectSql);
		PreparedStatement pstmt = conn.prepareStatement(selectSql);
		pstmt.setInt(1, Integer.valueOf(targId));		
		
		ResultSet rset = pstmt.executeQuery();
		while(rset.next())
		{
			double bid = rset.getDouble(1);
			double prc = rset.getDouble(2);
			int line_id = rset.getInt(3);
			
			if(targLineSet != null && !targLineSet.contains(line_id))
			{ 
				//Util.pf("\nDiscarding result for line_id=%d", line_id);
				continue; 
			}
			
			String prcStr = Util.sprintf("%.02f", prc);
			String gapStr = Util.sprintf("%.02f", bid-prc);
			//Util.pf("\nLine id is %d", line_id);
			
			Util.incHitMap(gapMap, gapStr);
			Util.incHitMap(prcMap, prcStr);
		}
		
		Util.pf("\nDB query took %.02f secs", (System.currentTimeMillis() - startTime)/1000);
		
		return hitmap;
	}
	
	String getSelectSql()
	{
		int targId = Integer.valueOf(optArgs.get("targ_id"));
		
		String whereclause = (targId < 1000000000 ? " camp_id = ? " : " line_id = ?");
		
		String selectsql = Util.sprintf("SELECT bid, price, line_id FROM price_info WHERE %s", whereclause);
		
		return selectsql;
	}
	
	boolean isCampQuery()
	{
		return getCampId() != null;
	}
	
	Integer getCampId()
	{
		int targId = Integer.valueOf(optArgs.get("targ_id"));
		if(targId < 1000000000)
			{ return Integer.valueOf(targId); }
		
		return null;
	}
	
	void genLineFilterSet() throws SQLException
	{
		Util.pf("\nCalling filter ");
		
		String targLineType = optArgs.get("line_type");
		
		if(!isCampQuery() || "all".equals(targLineType))
			{ return; }
				
		targLineSet = Util.treeset();
			
		int campId = getCampId();
		
		String selectSql = "SELECT line_id FROM line_info WHERE camp_id = ? AND line_type = ?";
		
		Connection conn = DbConnect.getConnection();

		// Util.pf("\nSelect sql is %s", selectSql);
		PreparedStatement pstmt = conn.prepareStatement(selectSql);
		pstmt.setInt(1, campId);
		pstmt.setString(2, targLineType);		
		
		ResultSet rset = pstmt.executeQuery();
		while(rset.next())
		{
			int hitLine = rset.getInt(1);
			targLineSet.add(hitLine);
			
			//Util.pf("\nFound line type %s for camp_id %d", targLineType, campId);
		}
		
		if(targLineSet.size() == 0)
			{ throw new RuntimeException("\nNo line items of type " + targLineType + " found for campaign " + campId); }
		
		Util.pf("\nFound filter set: %s", targLineSet);
		//Util.pf("\nDB query took %.02f secs", (System.currentTimeMillis() - startTime)/1000);
	}
}

