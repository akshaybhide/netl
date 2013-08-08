package com.adnetik.bm_etl;

import java.util.*;
import java.sql.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*; 
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

/**
 * Cost Calc Test
 * Run some code that checks the views that calculate cost are correct.
 */
public class CostCalcTest 
{
	
	public static final int LIMIT_CHECK = 1000;
	
	Map<Long, LineData> lineMap = Util.treemap();
	
	public static void main(String[] args) throws Exception
	{
		CostCalcTest cct = new CostCalcTest();
		Set<Integer> campset = getCampaignIds();
		Integer[] touse = campset.toArray(new Integer[2]);
		//int[] touse = new int[] { 96, 802, 904 };
		
		for(int campid : touse)
		{
			Util.pf("\nRunning check for campid=%d", campid);
			cct.basicTest(campid);
		}
	}
	
	
	private static Connection _MAIN_CONN;
	private static Connection _ADN_CONN;
	
	private static Connection subAdnConn() throws SQLException
	{
		if(_ADN_CONN == null || _ADN_CONN.isClosed())
			{ _ADN_CONN = DatabaseBridge.getAdnConnection(DbTarget.external); }
		
		return _ADN_CONN;		
	}
	
	private static Connection subConn() throws SQLException
	{
		if(_MAIN_CONN == null || _MAIN_CONN.isClosed())
			{ _MAIN_CONN = DatabaseBridge.getDbConnection(DbTarget.external); }
		
		return _MAIN_CONN;
	}
	
	public static Set<Integer> getCampaignIds() throws SQLException
	{
		Set<Integer> campids = Util.treeset();
		Connection conn = subConn();
		String sql = Util.sprintf("SELECT distinct(id_campaign) FROM ad_general");
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		
		while(rset.next())
		{
			campids.add(rset.getInt(1));
		}		
		
		conn.close();
		
		return campids;
	}
	
	public void basicTest(int campid) throws Exception
	{
		Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
		String sql = Util.sprintf("SELECT * FROM ad_general_all WHERE num_impressions > 0 AND id_campaign = %d", campid);
		// String sql = Util.sprintf("SELECT * FROM ad_general_all WHERE num_impressions > 0 AND id_campaign = 1322 and id_date = date('2012-02-29')");
		//String sql = Util.sprintf("SELECT * FROM ad_general_all WHERE num_impressions > 0 AND id_campaign = 1271 limit %d", LIMIT_CHECK);
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		
		int ccount = 0;
		
		while(rset.next())
		{
			CostSub csub = new CostSub(rset);
			
			csub.checkIncData();
			csub.checkEffView(rset);
			csub.checkFinalCalc(rset); 
			
			ccount++;
			
			if((ccount % 10000) == 0)
			{
				Util.pf("\nCheck %d rows", ccount);
			}
		}
		
		Util.pf("\nChecked %d rows total", ccount);
		conn.close();
	}
	
	public class CostSub
	{
		Long lineItem;
		Integer campId;
		
		Double incDataCost;
		
		LineData relLine;
		
		Double effCost;
		String effUnit;
		Integer effType;

		// facts
		Integer numImp;
		Integer numClick;
		Double impCost;
		
		public CostSub(ResultSet rset) throws SQLException
		{
			impCost = grabDouble(rset, "IMP_COST");
			lineItem = grabLong(rset, "ID_LINEITEM");
			campId = grabInteger(rset, "ID_CAMPAIGN");
			numImp = grabInteger(rset, "num_impressions");
			numClick = grabInteger(rset, "num_clicks");
			
			checkNonNull(rset);
			
			incDataCost = grabDouble(rset, "incd_cost");
			
			
			// Util.pf("\nFound line item %d with cost %.05f", lineItem, impCost);
			
			lookupLineData(lineItem);
			
			relLine = lineMap.get(lineItem);
			
			effCost = grabDouble(rset, "effc_cost");
			effType = grabInteger(rset, "effc_type");
		}
		
		void checkNonNull(ResultSet rset) throws SQLException
		{
			if(impCost == null || lineItem == null || campId == null || numImp == null || numClick == null)
			{
				Util.pf("\nFound row with unacceptable null values");
				printResultRow(rset);
			}
		}
		
		void checkIncData()
		{
			Double a = incDataCost;
			Double b = relLine.getIncData();
			Util.massert((a == null && b == null) || Math.abs(a - b) < 1e-8, "Inc data field inconsistent");
		}
		
		void checkEffView(ResultSet rset) throws SQLException
		{
			checkPrintSame(effCost, relLine.getEffCost(), rset, " effect cost");
			checkPrintSame(effType, relLine.getEffType(), rset, " effect type");
		}
		
		void checkFinalCalc(ResultSet rset) throws SQLException
		{
			Double drv_cpm = grabDouble(rset, "cpm_cost");
			Double drv_cpc = grabDouble(rset, "cpc_cost");
			
			Double raw_cpm = (effCost == null ? null : effCost * numImp);
			Double raw_cpc = (effCost == null ? null : effCost * numClick); 
			
			checkPrintSame(drv_cpm, raw_cpm, rset, " CPM calc ");
			checkPrintSame(drv_cpc, raw_cpc, rset, " CPC calc ");

			Double raw_form_1 = (effCost == null ? null : impCost * (1 + effCost));
			Double raw_form_2 = (effCost == null ? null : impCost + (effCost * numImp) / 1000);
			
			Double drv_form_1 = grabDouble(rset, "dcpm_form1");
			Double drv_form_2 = grabDouble(rset, "dcpm_form2");
				
			checkPrintSame(raw_form_1, drv_form_1, rset, " DCPM formula 1");
			checkPrintSame(raw_form_2, drv_form_2, rset, " DCPM formula 1");
			
			// TODO: this should probably be called client_cost or cost_two
			Double drv_final = grabDouble(rset, "final_cost");

			Double raw_final = null;	
			if(effType != null)
			{
				if(effType == CreateViews.CPM_ID_CODE) 
					{ raw_final = raw_cpm; }
				
				if(effType == CreateViews.CPC_ID_CODE)
					{ raw_final = raw_cpc; }
				
				if(effType == CreateViews.DCPM_ID_CODE_SYNTH_F1)
					{ raw_final = raw_form_1; }
				
				if(effType == CreateViews.DCPM_ID_CODE_SYNTH_F2)
					{ raw_final = raw_form_2; }
			}
			
			// Util.pf("\nDrvFinal = %s, raw_final = %s", drv_final, raw_final);
			
			checkPrintSame(drv_final, raw_final, rset, "Final Cost");
			
		}		
		
		void checkPrintSame(Object a, Object b, ResultSet rset, String errcode) throws SQLException
		{
			boolean same = checkSame(a, b);
			
			if(!same)
			{
				printResultRow(rset);
				Util.massert(false, "Problem with code " + errcode);
			}
		}
	}
	
	void lookupLineData(long lineid) throws SQLException
	{
		if(lineMap.containsKey(lineid))
			{ return; }
		
		LineData ldata = new LineData(lineid);
		lineMap.put(lineid, ldata);
	}
	
	private static Map<Integer, String> _TYPE_MAP;
	
	static Map<Integer, String> getTypeMap() throws SQLException
	{
		if(_TYPE_MAP == null)
		{
			 _TYPE_MAP = Util.treemap();
			Connection conn = DatabaseBridge.getAdnConnection(DbTarget.external);
			String sql = Util.sprintf("SELECT id, name FROM cost_type");
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();		
			
			while(rset.next())
			{
				_TYPE_MAP.put(rset.getInt(1), rset.getString(2));
			}
			
			conn.close();
		}
		
		return _TYPE_MAP;
	}
	
	public static class LineData
	{
		int campId;
		long lineId; 
		Double lineCost;
		Integer lineUnit;
		Integer lineType;
		Double lineData;
		
		Double campCost;
		Integer campUnit;
		Integer campType;
		
		public LineData(long lid) throws SQLException
		{
			lineId = lid;
			
			Connection conn = DatabaseBridge.getAdnConnection(DbTarget.external);
			String sql = "SELECT *, CMP.cost campcost, CMP.cost_unit campunit, CMP.cost_type_id camptype ";
			sql += " FROM line_item INNER JOIN campaign CMP on line_item.campaign_id = CMP.id WHERE line_item.id = ?";
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setLong(1, lid);
			
			ResultSet rset = pstmt.executeQuery();
			
			Util.massert(rset.next(), "Line item not found %d", lid);
			
			lineCost = grabDouble(rset, "cost");
			lineUnit = grabInteger(rset, "cost_unit"); 
			lineType = grabInteger(rset, "cost_type_id");
			lineData = grabDouble(rset, "data_cost");			
			
			campCost = grabDouble(rset, "campcost");
			campUnit = grabInteger(rset, "campunit");
			campType = grabInteger(rset, "camptype");
			
			conn.close();
		}
		
		public Double getIncData()
		{
			if(lineCost == null)
				{ return null; }
			
			return lineCost + (lineData == null ? 0 : lineData);
		}
		
		public Double getEffCost()
		{
			Double icd = getIncData();

			return (icd == null ? campCost : icd);
		}
				
		public Integer getEffType()
		{
			return (lineType == null ? getSynthType(campType, campUnit) : getSynthType(lineType, lineUnit));
		}
		
		public Integer getSynthType(Integer rtype, Integer runit)
		{
			if(rtype == null)
				{ return null; }
			
			Util.massert(false, "Need to rewrite this code");
			/*
			if(rtype == CreateViews.DCPM_ID_CODE_BASE)
			{
				return (runit == CreateViews.UNIT_PERC_CODE ? 
					CreateViews.DCPM_ID_CODE_SYNTH_F1 : CreateViews.DCPM_ID_CODE_SYNTH_F2);
			}
			*/
			
			return rtype;
		}
	}
	
	static Integer grabInteger(ResultSet rset, String colcode) throws SQLException
	{
		String x = rset.getString(colcode);
		return (x == null ? null : Integer.valueOf(x));				
	}
	
	static Long grabLong(ResultSet rset, String colcode) throws SQLException
	{
		String x = rset.getString(colcode);
		return (x == null ? null : Long.valueOf(x));				
	}	
	
	static Double grabDouble(ResultSet rset, String colcode) throws SQLException
	{
		String x = rset.getString(colcode);
		return (x == null ? null : Double.valueOf(x));		
	}
	
	static boolean checkSame(Object a, Object b)
	{
		if(a == null || b == null)
		{
			return ((a == null) == (b == null));
		}
		
		if(a instanceof String || a instanceof Integer)
			{ return a == b; }
		
		if(a instanceof Double)
			{ return Math.abs( ((Double) a) - ((Double) b) ) < 1e-9; }
		
		throw new RuntimeException("Unknown type of left value " + a.getClass());
	}
		
	static void assertSame(Object a, Object b, String errcode)
	{
		Util.massert(checkSame(a, b), "Values not same a=%s, b=%s for code %s", a, b, errcode);	
	}
	
	static void printResultRow(ResultSet rset) throws SQLException
	{
		ResultSetMetaData rsmd = rset.getMetaData();
		int colcount = rsmd.getColumnCount();

		Util.pf("\nResult set info: ");
		
		for(int i = 1; i <= colcount; i++)
		{
			String colid = rsmd.getColumnName(i);
			String colval = rset.getString(i);
			
			Util.pf("\n\tcolname=%s, value=%s", colid, colval);
		}
	}

}
