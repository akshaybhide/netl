/**
 * 
 */
package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


public class UnitTest extends Configured implements Tool 
{
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args)
	{
		for(String oneday : TimeUtil.getDateRange("2012-03-15", "2012-03-26"))
		{
			dayByDayCheck(oneday);
		}
		
		// campByCampCheck(1079);
		// campByCampCheck(988);
		/*
		String daycode = "2012-03-15";
		
		CatalogUtil.initSing(daycode);
		
		for(DimCode dc : DimCode.values())
		{
			if(dc.hasCatalog())
				{ Util.pf("\nFound %d entries for catalog %s", CatalogUtil.getSing().logCodeMap.get(dc).size(), dc);} 
		}
		
		testCurrency();
		*/
		
		/*
		Set<DimCode> nocheck = Util.treeset();
		nocheck.add(DimCode.gender);
		nocheck.add(DimCode.city);
		
		for(DimCode dcode : DimCode.values())
		{
			if(nocheck.contains(dcode))
				{ continue; }
			
			if(dcode.hasCatalog())
				{ checkCatConsistency(dcode); }
		}
		*/
		//testCampaignIdKey();
		
		return -1;
	}
	
	void campByCampCheck(int campId) 
	{
		/*
		Map<String, Number[]> dommap = DatabaseBridge.campaignCountCheck(AggType.ad_domain, campId);
		Util.pf("Finished ad_domain query\n");
		Map<String, Number[]> genmap = DatabaseBridge.campaignCountCheck(AggType.ad_general, campId);
		Util.pf("Finished ad_general query\n");
		int[] okkerr = checkCountMaps(genmap, dommap);
		Util.pf("Found %d okay, %d err for campid=%s\n", okkerr[0], okkerr[1], ""+campId);
		*/
	}
	
	void dayByDayCheck(String daycode)
	{
		/*
		Map<String, Number[]> dommap = DatabaseBridge.dateCountCheck(AggType.ad_domain, daycode);
		Util.pf("Finished ad_domain query\n");
		Map<String, Number[]> genmap = DatabaseBridge.dateCountCheck(AggType.ad_general, daycode);
		Util.pf("Finished ad_general query\n");
		int[] okkerr = checkCountMaps(genmap, dommap);
		Util.pf("Found %d okay, %d err for daycode=%s\n", okkerr[0], okkerr[1], daycode);
		*/		
	}
	
	int[] checkCountMaps(Map<String, Number[]> genmap, Map<String, Number[]> dommap)
	{
		int errcount = 0;
		int okkcount = 0;
		
		Map<String, Number[]> relmap = (genmap.size() > dommap.size() ? genmap : dommap);
		
		for(String key : relmap.keySet())
		{
			if(!dommap.containsKey(key))
			{ 
				Util.pf("ERROR: ad_domain does not contain key %s\n", key); 
				errcount++;
				continue;
			}			
			
			if(!genmap.containsKey(key))
			{ 
				Util.pf("ERROR: ad_general does not contain key %s\n", key);
				errcount++;
				continue;
			}
			
			Number[] domdata = dommap.get(key);
			Number[] gendata = genmap.get(key);
			boolean perfect = true;
			
			for(int i = 0; i < gendata.length; i++)
			{
				if(domdata[i] instanceof Integer)
				{				
					int delta = ((Integer) gendata[i]) - ((Integer) domdata[i]);
					
					if(delta != 0)
					{ 
						Util.pf("Error for key %s, Delta is %d , (%d, %d)\n", key, delta, domdata[i], gendata[i]);	
						perfect = false;
					}
				} else {
					
					double delta = gendata[i].doubleValue() - domdata[i].doubleValue();
					
					if(Math.abs(delta) > 1)
					{ 
						Util.pf("Error for key %s, Delta is %.03f , (%.03f, %.03f)\n", key, delta, domdata[i], gendata[i]);	
						perfect = false;
					}					
				}
			}
			
			errcount += (perfect ? 0 : 1);
			okkcount += (perfect ? 1 : 0);
		}				
		return new int[] { okkcount, errcount };
	}
	
	void testCampaignIdKey()
	{
		String sqlB = "SELECT distinct(id) FROM adnetik.campaign";
		SortedSet<String> cmpset = distinctQuery(sqlB);	
		Util.pf("\nFound %d campaign IDS in main adnetik database.", cmpset.size());		
		
		Util.pf("\nCampId set is %s", cmpset);
		
		String sqlA = "SELECT distinct(id_campaign) FROM ad_general";
		SortedSet<String> adgset = distinctQuery(sqlA);
		Util.pf("\nFound %d distinct campaign IDs in ad_general", adgset.size());
		
		for(String adgid : adgset)
		{
			Util.pf("\nFound campid=%s from ad_general" , adgid);
			Util.massert(cmpset.contains(adgid), "Could not find ID=%s in main campaign table", adgid);	
		}
	}
	
	void testCurrency()
	{
		Random jRand = new Random();
		CatalogUtil cutil = CatalogUtil.getSing();
		CurrCode[] currs = CurrCode.values();
		
		for(int i = 0; i < 10000; i++)
		{
			double x = 100 * jRand.nextDouble();
			
			// Converting from USD to USD should give back same answer
			for(CurrCode cc : currs)
			{
				double y = cutil.convertA2B(cc, cc, x);
				Util.massert(Math.abs(x - y) < 1e-9);
			}
			
			CurrCode movethru = currs[jRand.nextInt(currs.length)];
			
			for(CurrCode cc : currs)
			{
				// Test back and forth conversion
				double far = cutil.convertA2B(cc, movethru, x);
				double near = cutil.convertA2B(movethru, cc, far);
				Util.massert(Math.abs(x - near) < 1e-9);
			}
		}
		
		Util.pf("\nCurrency check passed\n");
	}
	
	// This is a poor man's version of foreign key references 
	static void checkCatConsistency(DimCode tocheck)
	{
		Set<String> nameset = Util.treeset();
		String sql = Util.sprintf("SELECT distinct(name_%s) FROM ad_general_all WHERE id_campaign = 1306", tocheck);
		
		nameset = distinctQuery(sql);
		Util.pf("\nFound %d distinct values for dimension %s", nameset.size(), tocheck);
	}
	
	static SortedSet<String> distinctQuery(String sql)
	{
		SortedSet<String> qset = Util.treeset();
		
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				String s  = rset.getString(1);	
				Util.massert(s != null, " Found null value for query %s", sql);
				qset.add(s);
			}
			
			conn.close();
			return qset;
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);	
		}		
	}
}
