package com.adnetik.bm_etl;

import java.util.*;
import java.sql.*;
import java.io.*;

import com.adnetik.shared.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


/**
 * DatabaseBridge
 */
public class CsvReport 
{
	public enum Dim { adexchange, browser, advertiser, campaign, date, city, country, creative, domain, exchange, gender, language, lineitem, metrocode, region, size, visibility };
	
	public enum Fact { num_impressions, imp_cost, num_clicks, num_conversions, num_conv_post_view, num_conv_post_click };
	
	//static Map<
	
	static Map<Dim, Map<Integer, String>> DIM_CAT_MAP = Util.treemap();
	
	static Set<Dim> _HAS_CATALOG;
	
	public static boolean hasCatalog(Dim d)
	{
	 	if(_HAS_CATALOG == null)
	 	{
	 		_HAS_CATALOG = Util.treeset();
	 		_HAS_CATALOG.add(Dim.browser);
	 		_HAS_CATALOG.add(Dim.country);
	 		_HAS_CATALOG.add(Dim.domain);
	 		_HAS_CATALOG.add(Dim.exchange);
	 		_HAS_CATALOG.add(Dim.gender);
	 		_HAS_CATALOG.add(Dim.language);
	 		_HAS_CATALOG.add(Dim.region);
	 		_HAS_CATALOG.add(Dim.size);
	 		_HAS_CATALOG.add(Dim.visibility);
	 	}
	 	
	 	return _HAS_CATALOG.contains(d);
		
	}
	
	public static Map<Integer, String> getDimCatMap(Dim onedim)
	{
		return DIM_CAT_MAP.get(onedim);	
	}
	
	public static void popCatMap(Dim onedim) throws SQLException
	{
		Util.massert(hasCatalog(onedim), "Don't call for dimensions that don't use catalogs");
		
		String sql = "select id, name from cat_" + onedim;
		Map<Integer, String> catmap = Util.treemap();
		
		Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		
		while(rset.next())
		{
			int id = rset.getInt(1);
			String s = rset.getString(2);
			catmap.put(id, s);
		}

		DIM_CAT_MAP.put(onedim, catmap);		
		
		//Util.pf("\nPopulated catmap for %s, have %d entries", onedim, catmap.size());
	}

	public static void standardQuery(List<Fact> factlist, List<Dim> dimlist, int campid, int alpha, int omega) throws SQLException
	{
		for(Dim onedim : dimlist)
		{
			if(hasCatalog(onedim))
				{ popCatMap(onedim); }
		}
		
		int colsize = factlist.size() + dimlist.size();
		
		Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
		
		String sql = getQuerySql(factlist, dimlist);
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		pstmt.setInt(1, campid);
		pstmt.setInt(2, alpha);
		pstmt.setInt(3, omega);
		
		
		ResultSet rset = pstmt.executeQuery();
		
		while(rset.next())
		{
			List<String> row = Util.vector();
			
			int colid = 1;
			
			for(Fact fact : factlist)
			{
				row.add("" + rset.getDouble(colid++));
			}
			
			for(Dim onedim : dimlist)
			{
				int rowid = rset.getInt(colid++);
				
				if(hasCatalog(onedim))
				{
					if(!DIM_CAT_MAP.get(onedim).containsKey(rowid))
					{ 
						Util.pf("\nNo id found for %s, id=%d", onedim, rowid);
						System.exit(1);
					}					
					
					row.add(DIM_CAT_MAP.get(onedim).get(rowid));
				} else {

					row.add(""+rowid);
				}
			}
				
			String rowstr = row.toString();
			rowstr = rowstr.substring(1, rowstr.length()-1);
			Util.pf("%s\n", rowstr);
		}
		
		conn.close();
	}

	public static String getQuerySql(List<Fact> factlist, List<Dim> dimlist)
	{
		// TODO need Jclose!!!
		List<String> slist = Util.vector();
		
		for(Dim d : dimlist)
			{ slist.add("id_" + d); } 
		
		
		List<String> flist = Util.vector();
		
		for(Fact f : factlist)
			{ flist.add(f.toString()); }
		
		return getQuerySqlSub(flist, slist);
	}
	
	
	
	private static String getQuerySqlSub(List<String> factlist, List<String> dimlist)
	{
		StringBuilder sb = new StringBuilder();
		
		
		List<String> slist = Util.vector();
		
		for(String fact : factlist)
		{
			slist.add("sum(" + fact + ")");	
		}
		
		for(String dim : dimlist)
		{
			slist.add(dim.toString());	
		}		
		
		String select = Util.join(slist.toArray(new String[1]), ",");
		
		String sql = Util.sprintf("SELECT %s FROM ad_general WHERE id_campaign = ? and ? <= id_date and id_date <= ? group by %s", 
			select, Util.join(dimlist, ","));
		
		return sql;
		
	}
	
	public static void main2(String[] args) throws Exception
	{
		List<Fact> factlist = Util.vector();
		List<Dim> dimlist = Util.vector();
		
		factlist.add(Fact.num_impressions);
		factlist.add(Fact.num_clicks);
		factlist.add(Fact.imp_cost);
		
		dimlist.add(Dim.browser);
		dimlist.add(Dim.country);
		dimlist.add(Dim.lineitem);
		
		//Util.pf("\nQuery is %s\n", getQuerySql(factlist, dimlist));
		
		standardQuery(factlist, dimlist, 1306, 20120201, 20120204);

		//popCatMap(Dim.browser);
	}
	
	
	public static void main(String[] args) throws Exception
	{
		List<Fact> factlist = Util.vector();
		List<Dim> dimlist = Util.vector();
		
		int campid = Integer.valueOf(args[0]);
		int alpha = Integer.valueOf(args[1]);
		int omega = Integer.valueOf(args[1]);
		
		for(int i = 3; i < args.length; i++)
		{
			try { 
				Dim d = Dim.valueOf(args[i]); 
				dimlist.add(d);
				continue;
			} catch (Exception ex) {}
			
			try { 
				Fact f = Fact.valueOf(args[i]); 
				factlist.add(f);
				continue;
			} catch (Exception ex) {}
			
			throw new RuntimeException("Argument not recognized as fact or dimension: " + args[i]);
		}
		
		if(factlist.size() == 0 || dimlist.size() == 0)
		{
			Util.pf("\nMust supply at least one dimension and fact.");	
		}
				
		//Util.pf("\nQuery is %s\n", getQuerySql(factlist, dimlist));
		
		standardQuery(factlist, dimlist, campid, alpha, omega);
	}
	
	
	public static void main4(String[] args) throws Exception
	{
		String sql = "SELECT DISTINCT(id_campaign) FROM ad_general";
		
		Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
				
		PreparedStatement pstmt = conn.prepareStatement(sql);
		
		Set<Integer> campidset = Util.treeset();
		
		ResultSet rset = pstmt.executeQuery();
				
		while(rset.next())
		{
			campidset.add(rset.getInt(1));
			//Util.pf("\n%s", rset.getString(1));
		}
		
		for(Integer id : campidset)
		{
			Util.pf("%d\n", id);	
			
		}
		
	}


	
	
}
