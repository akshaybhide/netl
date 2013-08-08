

package com.adnetik.bm_etl;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.Util;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

public class CreateViews
{
	public static final int CPM_ID_CODE = 1;
	public static final int CPC_ID_CODE = 2;
	
	public static final String UNIT_PERC_CODE = "%";
	public static final String UNIT_DOLL_CODE = "$";
	
	public static final int DCPM_ID_CODE_BASE = 3;
	public static final int DCPM_ID_CODE_SYNTH_F1 = 4;
	public static final int DCPM_ID_CODE_SYNTH_F2 = 5;
	
	public static final int APPNEXUS_ID = 5;
	
	String baseTableName; 

	public static void main(String[] args) throws Exception
	{
		AggType[] runtype = new AggType[] { AggType.ad_general, AggType.ad_domain };
		// dropViews();
		
		for(AggType atype : runtype)
		{
			int stacksize = (atype == AggType.ad_general ? 5 : 4);
			ViewStack vstack = new ViewStack(atype.toString(), stacksize);
			
			CreateViews cview = new CreateViews(atype);
			
			if(atype == AggType.ad_general)
			{
				cview.buildCurrencyViews();
				cview.buildSynthTypeViews();
				cview.buildIncDataView();
				cview.buildEffLineCostView();
				cview.buildAppNexusAsstView();
				//cview.buildFoldAsstIdB(vstack);
				//cview.buildFoldAsstIdA(vstack);
			}
			
			cview.foldEffLineItem(vstack);
			cview.buildCpmCalcView(vstack);
			cview.buildFinalCostInfo(vstack);
			cview.buildFinalCurrInfo(vstack);
			
			if(atype == AggType.ad_general)
				{ cview.buildAss2CtvView(vstack); }	

			
			Util.massert(vstack.finished(), "Stack not finished");
			
			// TODO: put this view underneath some others, so that 
			// the final, relevant view for both ad_domain and ad_general has
			// the same suffix.
			//cview.buildNameView();
		}
	}
	
	public CreateViews(AggType atype)
	{
		baseTableName = atype.toString();
	}
		
	// This folds in the currency info for EUR and GBP, it is the last view.
	void buildFinalCurrInfo(ViewStack vstack) throws SQLException
	{
		String[] lowhigh = vstack.pollStackLowHigh();
		String sql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT AGC.*, ", lowhigh[1]);
		sql += " AGC.final_cost_usd * XEUR.one_usd_buys as final_cost_eur, ";
		sql += " AGC.final_cost_usd * XGBP.one_usd_buys as final_cost_gbp ";
		sql += Util.sprintf(" FROM %s AGC ", lowhigh[0]); 
		sql += " LEFT JOIN v__exchange_eur XEUR ON AGC.id_date = XEUR.id_date "; 
		sql += " LEFT JOIN v__exchange_gbp XGBP ON AGC.id_date = XGBP.id_date "; 
		 
		basicExec(sql);
	}		
	
	void buildAss2CtvView(ViewStack vstack ) throws SQLException
	{
		Util.pf("going to build a ass2ctv view\n");
		String[] lowhigh = vstack.pollStackLowHigh();
		Util.pf("\nLow-high is %s , %s", lowhigh[0], lowhigh[1]);
		
		String sql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT ADG.*, ", lowhigh[1]);
		sql += Util.sprintf(" IF(ADG.id_exchange = %d, APPN.creative_id, REG.creative_id) AS REAL_CRTV_ID ", APPNEXUS_ID);
		sql += Util.sprintf(" FROM %s ADG ", lowhigh[0]);
		sql += " LEFT JOIN appnexus_creative_lookup APPN ON ADG.id_creative = APPN.appnexus_id ";
		sql += " LEFT JOIN adnetik.assignment REG ON ADG.id_creative = REG.id ";
		 
		// Util.pf("\nSQL is \n\t%s\n", sql);
		basicExec(sql);
	}			
	
	void buildFinalCostInfo(ViewStack vstack) throws SQLException
	{
		String[] lowhigh = vstack.pollStackLowHigh();
		String sql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT CPMV.*, ", lowhigh[1]);
		sql += " (CASE CPMV.effc_type ";
		sql += Util.sprintf(" WHEN %d THEN CPMV.cpm_cost ", CPM_ID_CODE);
		sql += Util.sprintf(" WHEN %d THEN CPMV.cpc_cost ", CPC_ID_CODE); 
		sql += Util.sprintf(" WHEN %d THEN CPMV.dcpm_form1 ", DCPM_ID_CODE_SYNTH_F1);
		sql += Util.sprintf(" WHEN %d THEN CPMV.dcpm_form2 ", DCPM_ID_CODE_SYNTH_F2);
		sql += Util.sprintf(" ELSE NULL END) final_cost_usd " );
		sql += Util.sprintf(" FROM %s CPMV ", lowhigh[0]);
		 
		basicExec(sql);
	}		
	
	// This uses conditional logic to choose the correct EFF_LINE_INFO view,
	// based on whether the EXT_LINEITEM is null. If null, we are dealing
	// with an external line item id.
	void foldEffLineItem(ViewStack vstack) throws SQLException
	{
		String[] lowhigh = vstack.pollStackLowHigh();

		String sql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT ADG.*, ", lowhigh[1]);
		sql += "IF(ADG.EXT_LINEITEM IS NULL, EFF_REG.effc_cost, EFF_EXT.effc_cost) AS effc_cost, ";
		sql += "IF(ADG.EXT_LINEITEM IS NULL, EFF_REG.effc_type, EFF_EXT.effc_type) AS effc_type,  ";
		sql += " 87878787 as campaign_id "; //TODO: this is a major hack, take it out!!!
		sql += Util.sprintf(" FROM %s ADG ", lowhigh[0]);	
		sql += " LEFT JOIN v__eff_line_ext EFF_EXT ON ADG.ext_lineitem = EFF_EXT.line_id ";	
		sql += " LEFT JOIN v__eff_line_reg EFF_REG ON ADG.id_lineitem  = EFF_REG.line_id ";	
		
		basicExec(sql);
	}			
	
	
	// Once each for ad_general and ad_domain
	// Fold in CPM info
	// TODO: unify this view with the one above
	void buildCpmCalcView(ViewStack vstack) throws SQLException
	{
		String[] lowhigh = vstack.pollStackLowHigh();
		String sql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT ADG.*,  ", lowhigh[1]);
		sql += "(ADG.num_impressions/1000) * effc_cost as cpm_cost, ";
		sql += "ADG.num_clicks * effc_cost cpc_cost, ";
		sql += "ADG.imp_cost * (1 + effc_cost/100) dcpm_form1, ";
		sql += "ADG.imp_cost + (effc_cost * ADG.num_impressions/1000) dcpm_form2 ";
		sql += Util.sprintf(" FROM %s ADG ", lowhigh[0]);		

		basicExec(sql);
	}		
	
	// Only run once
	void buildEffLineCostView() throws SQLException
	{
		String[] view_suff = new String[] { "reg" , "ext" };
		String[] join_clause = new String[2];
		
		join_clause[0] = "INNER JOIN v__campaign_synth CMP on IDV.campaign_id = CMP.id";
		join_clause[1] = "INNER JOIN adnetik.external_campaign ECMP ON IDV.external_campaign_id = ECMP.id INNER JOIN v__campaign_synth CMP ON ECMP.campaign_id = CMP.id";
		
		for(int i = 0; i < 2; i++)
		{
			String sql = "CREATE OR REPLACE VIEW v__eff_line_" + view_suff[i] + " AS SELECT IDV.*, CMP.cost as camp_cost, CMP.synth_type as camp_type, ";
			sql += "IFNULL(IDV.incd_cost, CMP.cost) effc_cost, ";
			sql += "IFNULL(IDV.line_type, CMP.synth_type) effc_type ";
			sql += Util.sprintf(" FROM v__inc_data_%s IDV %s", view_suff[i], join_clause[i]);
			basicExec(sql);			
		}
	}		
	
	// Once only
	void buildCurrencyViews() throws SQLException
	{
		for(CurrCode onecode : new CurrCode[] { CurrCode.EUR, CurrCode.GBP })
		{
			String sql = Util.sprintf("CREATE OR REPLACE VIEW v__exchange_%s AS SELECT id_date, one_usd_buys FROM daily_exchange_rates WHERE curr_code = '%s'",
					onecode.toString().toLowerCase(), onecode);
			
			basicExec(sql);
		}
	}		
	
	// Also only needs to be run once.
	void buildIncDataView() throws SQLException
	{
		{
			String sql = Util.sprintf("CREATE OR REPLACE VIEW v__inc_data_reg AS SELECT id as line_id, campaign_id, cost as line_cost, synth_type as line_type, data_cost, cost+IF(data_cost is null, 0, data_cost) incd_cost FROM v__line_item_synth");
			basicExec(sql);
		} {
			String sql = Util.sprintf("CREATE OR REPLACE VIEW v__inc_data_ext AS SELECT id as line_id, external_campaign_id, cost as line_cost, synth_type as line_type, data_cost, cost+IF(data_cost is null, 0, data_cost) incd_cost FROM v__external_line_item_synth");
			basicExec(sql);		
		}
	}	

	// Also only needs to be run once.
	void buildAppNexusAsstView() throws SQLException
	{
		String sql = Util.sprintf("CREATE OR REPLACE VIEW v__appnexus_asst AS SELECT distinct(creative_id), appnexus_id FROM adnetik.assignment group by appnexus_id");
		basicExec(sql);
	}			
	
	// This only needs to be run once per table.
	void buildSynthTypeViews() throws SQLException
	{
		String[] tabnames = new String[] { "campaign", "line_item", "external_line_item" };
		
		for(String onetab : tabnames)
		{
			// The purpose of this "synthetic" cost_type column is to hide the dumbness 
			// implied by the use of a separate, redundant cost_unit column.
			// The DCPM code is replaced by a synthetic view code that is one of two values
			String costif = Util.sprintf(" IF(cost_type_id = %d, IF(cost_unit = '%s', %d, %d), cost_type_id) as synth_type ",
				DCPM_ID_CODE_BASE, UNIT_PERC_CODE, DCPM_ID_CODE_SYNTH_F1, DCPM_ID_CODE_SYNTH_F2);
			String sql = Util.sprintf("CREATE OR REPLACE VIEW v__%s_synth AS SELECT *,  %s FROM adnetik.%s",
				onetab, costif, onetab);
			
			Util.pf("SQL is %s\n", sql);	
						
			basicExec(sql);
		}
	}
	
	private void basicExec(String sql)
	{
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			sql = sql.replace("ad_general", baseTableName);
			
			Util.pf("\nResulting SQL is \n%s\n", sql);
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.executeUpdate();
			conn.close();			
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}
	
	static void dropViews()
	{
		List<String> viewlist = Util.vector();
		
		try {
			String sql = "select table_name from information_schema.tables where table_type = 'VIEW' and table_schema = 'bm_etl'";
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
				{ viewlist.add(rset.getString(1));	}
			
			for(String view : viewlist)
			{
				PreparedStatement deleter = conn.prepareStatement("DROP VIEW IF EXISTS " + view);
				deleter.executeUpdate();
			}
			conn.close();
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);	
		}
		
		Util.pf("View list is %s", viewlist);
		
		
	}	
	
	public static class ViewStack 
	{
		LinkedList<String> slist = Util.linkedlist();
		
		public ViewStack(String basename, int size)
		{
			slist.add(basename);
			
			for(int i = 1; i < size; i++)
			{ 
				String vname = Util.sprintf("v__%s_%d", basename, i); 
				slist.add(vname);
			}
			
			slist.add("v__" + basename + "_all");
		}
		
		public String[] pollStackLowHigh()
		{
			String low = slist.poll();
			String hgh = slist.peek();
			
			Util.massert(hgh != null, "Too many View stack calls");		
			return new String[] { low, hgh };
		}
		
		public boolean finished() { 
			return slist.size() == 1;	
		}
	}
	
	// ALL - adds name lookup to WCURR
	/*
	void buildNameView() throws SQLException
	{
		if(baseTableName.equals("ad_domain"))
		{ 
			String sql = "CREATE OR REPLACE VIEW v__ad_general_all AS SELECT * FROM v__ad_general_wcost";
			basicExec(sql);
			return; 
		}
		
		List<DimCode> catlist = Util.vector();
		catlist.add(DimCode.exchange);
		catlist.add(DimCode.metrocode);
		catlist.add(DimCode.region);
		catlist.add(DimCode.country);
		catlist.add(DimCode.size);
		catlist.add(DimCode.visibility);
		catlist.add(DimCode.browser);
		catlist.add(DimCode.language);
		// catlist.add(DimCode.domain);


		String sql = "CREATE OR REPLACE VIEW v__ad_general_all AS SELECT AGC.*, ";
		
		for(DimCode dc : catlist)
		{
			sql += Util.sprintf(" cat_%s.name as NAME_%s ,", dc.toString().toLowerCase(), dc.toString().toUpperCase());
		}
		
		sql += " LIT.name as NAME_LINEITEM, CMP.name as NAME_CAMPAIGN, CRTV.name as NAME_CREATIVE ";
		sql += " FROM v__ad_general_wcurr AGC "; 
		sql += " INNER JOIN adnetik.line_item LIT on AGC.id_lineitem = LIT.id ";	
		sql += " INNER JOIN adnetik.campaign CMP on AGC.id_campaign = CMP.id ";
		sql += " LEFT JOIN adnetik.creative CRTV on AGC.id_creative = CRTV.id ";
		
		
		for(DimCode dc : catlist)
		{
			sql += Util.sprintf(" INNER JOIN cat_%s on AGC.ID_%s = cat_%s.id ", dc, dc, dc);
		}
		
		basicExec(sql);
	}
	
	void testNameJoin() throws SQLException
	{
		Util.massert(baseTableName.equals("ad_general"));
		
		List<DimCode> catlist = Util.vector();
		catlist.add(DimCode.exchange);
		catlist.add(DimCode.metrocode);
		catlist.add(DimCode.region);
		catlist.add(DimCode.country);
		catlist.add(DimCode.size);
		catlist.add(DimCode.visibility);
		catlist.add(DimCode.browser);
		catlist.add(DimCode.language);

		String sql = "CREATE OR REPLACE VIEW v__test_name_join AS SELECT AGC.*, ";
		
		List<String> slist = Util.vector();
		
		for(DimCode dc : catlist)
		{
			slist.add(Util.sprintf(" cat_%s.name as NAME_%s ", dc.toString().toLowerCase(), dc.toString().toUpperCase()));
		}
		
		sql += Util.join(slist, ",");
		sql += " FROM v__ad_general_wcurr AGC "; 		
		
		for(DimCode dc : catlist)
		{
			sql += Util.sprintf(" INNER JOIN cat_%s on AGC.ID_%s = cat_%s.id ", dc, dc, dc);
		}
		
		basicExec(sql);
	}	
	*/	
	
	/*
	void buildFoldAsstIdA(ViewStack vstack ) throws SQLException
	{
		Util.pf("going to build a ass2ctv view\n");
		String[] lowhigh = vstack.pollStackLowHigh();
		Util.pf("\nLow-high is %s , %s", lowhigh[0], lowhigh[1]);
		
		String sql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT ADG.*, ", lowhigh[1]);
		sql += Util.sprintf(" REG.creative_id AS REG_CRTV_ID ");
		sql += Util.sprintf(" FROM %s ADG ", lowhigh[0]);
		sql += " LEFT JOIN adnetik.assignment REG ON ADG.id_creative = REG.id ";
		 
		basicExec(sql);
	}	

	void buildFoldAsstIdB(ViewStack vstack ) throws SQLException
	{
		Util.pf("going to build a ass2ctv view\n");
		String[] lowhigh = vstack.pollStackLowHigh();
		Util.pf("\nLow-high is %s , %s", lowhigh[0], lowhigh[1]);
		
		String sql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT ADG.*, ", lowhigh[1]);
		sql += Util.sprintf(" APPN.creative_id AS APPN_CRTV_ID " );
		sql += Util.sprintf(" FROM %s ADG ", lowhigh[0]);
		sql += " LEFT JOIN appnexus_creative_lookup APPN ON ADG.id_creative = APPN.appnexus_id ";
		// sql += " LEFT JOIN adnetik.assignment APPN ON ADG.id_creative = APPN.appnexus_id ";
		basicExec(sql);
	}	
	*/
		
	
}
