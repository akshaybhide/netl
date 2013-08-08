

package com.adnetik.hadtest;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;


// This is a test of the speed and accuracy of lookups into the MaxMind database. 
public class JoinViewTest
{
	/*

	public static void main(String[] args) throws Exception
	{
		buildIncDataView();
		buildEffLineCostView();
		buildCpmCalcView();
		buildFinalCostInfo();
	}
	
	static void buildFinalCostInfo() throws SQLException
	{
		Connection conn = DbUtil.getDbConnection(DbUtil.BM_ETL_DATABASE, "bm_etl");
		String sql = "CREATE OR REPLACE VIEW final_cost_view AS SELECT CPMV.*, ";
		sql += " (CASE CPMV.eff_type_id WHEN 1 THEN CPMV.cpm_cost WHEN 2 THEN CPMV.cpc_cost WHEN 3 THEN CPMV.dcpm_form1 ELSE NULL END) final_cost ";
		sql += " FROM cpm_calc_view CPMV ";		
		 
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();

		conn.close();
	}			

	static void buildCpmCalcView() throws SQLException
	{
		Connection conn = DbUtil.getDbConnection(DbUtil.BM_ETL_DATABASE, "bm_etl");
		String sql = "CREATE OR REPLACE VIEW cpm_calc_view AS SELECT ADG.*, EFF.*,  ";
		sql += "ADG.num_impressions * EFF.eff_cost as cpm_cost, ";
		sql += "ADG.num_clicks * EFF.eff_cost cpc_cost, ";
		sql += "ADG.imp_cost * (1 + EFF.eff_cost) dcpm_form1, ";
		sql += "ADG.imp_cost + (EFF.eff_cost * ADG.num_impressions/1000) dcpm_form2 ";
		//sql += "IF(EFF.eff_type_id = 1, cpm_cost, IF(EFF.eff_type_id
		sql += " FROM ad_general ADG INNER JOIN adnetik.eff_line_cost EFF on ADG.id_lineitem = EFF.line_id";		
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();

		conn.close();
	}		
	
	

	
	static void buildEffLineCostView() throws SQLException
	{
		Connection conn = DbUtil.getDbConnection(DbUtil.BM_ETL_DATABASE, "adnetik");
		String sql = "CREATE OR REPLACE VIEW eff_line_info AS SELECT IDV.*, CMP.cost cmpcost, CMP.cost_unit cmp_unit, CMP.cost_type_id cmp_type_id, ";
		sql += "IFNULL(IDV.inc_data_cost, CMP.cost) eff_cost, ";
		sql += "IFNULL(IDV.cost_unit, CMP.cost_unit) eff_unit, ";
		sql += "IFNULL(IDV.cost_type_id, CMP.cost_type_id) eff_type_id ";
		sql += " FROM inc_data_view IDV INNER JOIN campaign CMP on IDV.campaign_id = CMP.id";
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();

		conn.close();
	}		
	
	static void buildIncDataView() throws SQLException
	{
		Connection conn = DbUtil.getDbConnection(DbUtil.BM_ETL_DATABASE, "adnetik");
		String sql = Util.sprintf("CREATE OR REPLACE VIEW inc_data_view AS SELECT id as line_id, campaign_id, cost, cost_unit, cost_type_id, data_cost, cost+IF(data_cost is null, 0, data_cost) inc_data_cost FROM line_item");
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.executeUpdate();

		conn.close();
	}		
	*/
}
