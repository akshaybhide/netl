
package com.adnetik.userindex;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class FeatureInfo
{
	// public static final String DOMAIN_LIST_PATH = "/com/adnetik/resources/BigDomainList.txt";
	public static final String CATEG_LIST_PATH = "/com/adnetik/resources/DomainCateg.txt";
	public static final String EXELATE_TOP_IDS = "/com/adnetik/resources/exelate_top_ids.txt";
	public static final String HISPANIC_INFO_PATH = "/com/adnetik/resources/HispanicData.tsv";
	
	public static final String COUNTRY_REGION_LIST = "CountryRegion.txt";
	
	public static final String RESOURCE_BASE = "/com/adnetik/resources/";
	
	private static Set<String> _DOMAIN_SET;
	
	private static Map<Integer, String> _EXELATE_DATA; 
	
	private static Map<String, Set<String>> _DOMAIN_CATEG;
	
	private static Map<String, Double> _HISPANIC_DATA;
	
	private static List<String> _COUNTRY_REGION_LIST; 
	
	private static String getCtryDomainPath(String ctrycode)
	{
		return Util.sprintf("/com/adnetik/resources/topdomain_%s.txt", ctrycode);
	}
	
	private static Map<String, Long> getDomBidMap4Ctry(String ctrycode)
	{
		String domainpath = getCtryDomainPath(ctrycode);
		Map<String, Long> bidmap = Util.treemap();
		InputStream resource = (new FeatureInfo()).getClass().getResourceAsStream(domainpath);
		Scanner sc = new Scanner(resource, "UTF-8");
		
		while(sc.hasNextLine())
		{
			String[] dom_bid = sc.nextLine().trim().split("\t");
			bidmap.put(dom_bid[0], Long.valueOf(dom_bid[1]));
		}		
		sc.close();
		
		return bidmap;
	}
	
	public static Set<String> getDomainSet()
	{
		if(_DOMAIN_SET == null)
		{
			_DOMAIN_SET = new LinkedHashSet<String>();
			
			for(CountryCode onectry : UserIndexUtil.COUNTRY_CODES)
			{
				Map<String, Long> bidmap4ctry = getDomBidMap4Ctry(onectry.toString());	
				_DOMAIN_SET.addAll(bidmap4ctry.keySet());
			}
				
		}
		
		return _DOMAIN_SET;		
		
	}
	
	/*
	public static Set<String> getDomainSet()
	{
		if(_DOMAIN_SET == null)
		{
			_DOMAIN_SET = new LinkedHashSet<String>();
			
			InputStream resource = (new FeatureInfo()).getClass().getResourceAsStream(DOMAIN_LIST_PATH);
			Scanner sc = new Scanner(resource, "UTF-8");
			
			while(sc.hasNextLine())
			{
				String s = sc.nextLine();	
				_DOMAIN_SET.add(s.trim());
			}
		}
		
		return _DOMAIN_SET;
	}
	*/
	
	public static Map<Integer, String> getTopExelateCategories()
	{
		if(_EXELATE_DATA == null)
		{
			_EXELATE_DATA = Util.treemap();
			
			InputStream resource = (new FeatureInfo()).getClass().getResourceAsStream(EXELATE_TOP_IDS);
			Scanner sc = new Scanner(resource, "UTF-8");
			
			while(sc.hasNextLine())
			{
				String s = sc.nextLine().trim();
				if(s.length() == 0)
					{ continue; }
				
				String[] code_id = s.split("\t");
				
				if(code_id.length < 2)
				{
					Util.pf("\nBad line %s", s); 	
				}
				
				String idsub = code_id[1];
				idsub = idsub.substring(1, idsub.length()-1);
				
				_EXELATE_DATA.put(Integer.valueOf(idsub), code_id[0]);
			}
		}
		
		return _EXELATE_DATA;
	}
		
	public static Map<String, Double> hispanicDensityByZip()
	{
		if(_HISPANIC_DATA == null)
		{
			_HISPANIC_DATA = new ConcurrentHashMap<String, Double>();
			
			List<String> hisplines = readResourceLines(HISPANIC_INFO_PATH);
			
			for(String oneline : hisplines)
			{
				// Zip, State, hisp pop, total pop
				String[] toks = oneline.split("\t");
				String zip = toks[0];
				double hpop = Double.valueOf(toks[2]);
				double tpop = Double.valueOf(toks[3]);
				_HISPANIC_DATA.put(zip, hpop/tpop);
			}
		}
		
		return _HISPANIC_DATA;		
	}
	
	public static Map<String, Set<String>> getDomainCats()
	{
		if(_DOMAIN_CATEG == null)
		{
			_DOMAIN_CATEG = Util.treemap();
			List<String> categs = readResourceLines(CATEG_LIST_PATH);

			for(String onecat : categs)
			{
				Set<String> doms = new TreeSet<String>(readResourceLines(RESOURCE_BASE + onecat + ".categ.txt"));
				_DOMAIN_CATEG.put(onecat, doms);
				
				// Util.pf("\nFound %d domains for category %s", doms.size(), onecat);
			}			
		}
		
		return _DOMAIN_CATEG;
	}

	public static synchronized List<String> getCountryRegionList()
	{
		if(_COUNTRY_REGION_LIST	== null)
		{
			Set<String> cset = Util.treeset();
			cset.addAll(readResourceLines(RESOURCE_BASE + COUNTRY_REGION_LIST));
			Util.pf("Read %d country-region pairs\n", cset.size());
			
			_COUNTRY_REGION_LIST = Util.vector();
			_COUNTRY_REGION_LIST.addAll(cset);
		}
		
		return Collections.unmodifiableList(_COUNTRY_REGION_LIST);
	}
	
	private static List<String> readResourceLines(String respath)
	{
		List<String> lines = Util.vector();
		InputStream resource = (new FeatureInfo()).getClass().getResourceAsStream(respath);
		Scanner sc = new Scanner(resource, "UTF-8");
		
		while(sc.hasNextLine())
		{
			String s = sc.nextLine().trim();
			lines.add(s);
		}				
		sc.close();
		
		return lines;
	}
	
	public static void main(String[] args)
	{	
		Set<String> bigdomain = getDomainSet();
		
		Util.pf("Big domain set is %d\n", bigdomain.size());
		
	}
}
