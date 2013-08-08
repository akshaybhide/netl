
package com.adnetik.bm_etl;

import java.sql.*;
import java.util.*;
import java.io.*;


import com.adnetik.shared.*;
import com.adnetik.bm_etl.BmUtil.*;

public class Maxmind
{	
	BufferedReader _mmReader;
	List<String> _infLines = Util.vector();
	int _batchCount = 0;
	
	Long _lastMin = 0L;
	
	private static final String CSV_PATH = "/mnt/adnetik/adnetik-uservervillage/maxmind/GeoIP-139/GeoIP-139_20120619/GeoIPCity.csv";
	
	public Maxmind() 
	{
	}
	
	void csv2inf() throws IOException
	{
		int lcount = 0;
		
		while(true)
		{
			String oneline = nextLine();
			if(oneline == null)
				{ break; }
			
			lcount++;
			
			if((lcount % 100) == 0)
			{
				Util.pf("Read line %d\n", lcount);	
			}
			
			_infLines.add(transformLine(oneline));
			
			
			if(_infLines.size() >= 10000)
				{ uploadInf(); }
			
			/*
			if(_infLines.size() >= 100000)
			{
				uploadInf();
				break; 
			}
			*/
		}
		
	
	}
	
	void uploadInf()
	{
		String[] colnames = new String[] { "min_ip", "max_ip", "country", "region", 
		"city", "postal", "latitude", "longitude", "dmacode", "areacode" };
		
		String fname = Util.sprintf("inf/local_maxmind_inf_%d.tsv", _batchCount);
		
		FileUtils.writeFileLinesE(_infLines, fname);
		
		int uprows = DbUtil.loadFromFile(new File(fname), "maxmind.ip_to_geo", 
				Arrays.asList(colnames), new DatabaseBridge(DbTarget.external));
		
		if(uprows != _infLines.size())
		{
			
			
		}
		Util.massert(uprows == _infLines.size(), "Wanted to upload %d entries, but only got %d on batchcount %d", 
			_infLines.size(), uprows, _batchCount);
		
		_infLines.clear();
		_batchCount++;
	}
	
	String transformLine(String csvline)
	{
		String[] toks = csvline.split(",");
		
		{
			long newmin = Util.ip2long(toks[0]);
			Util.massert(newmin > _lastMin);
			_lastMin = newmin;
			
			toks[0] = Util.ip2long(toks[0])+"";
			toks[1] = Util.ip2long(toks[1])+"";
		}
		
		for(int s = 2; s <= 5; s++)
		{ 
			toks[s] = toks[s].substring(1, toks[s].length()-1);	
		}
		
		return Util.join(toks, "\t");		
	}
	
	
	String nextLine() throws IOException
	{
		if(_mmReader == null)
		{ 
			_mmReader = Util.getReader(CSV_PATH); 
			String header = _mmReader.readLine();
			Util.massert(header.indexOf("country") > -1);
		}
		
		return _mmReader.readLine();	
	}
	
	
	public static void main(String[] args) throws Exception
	{
		Util.pf("Going to do a Maxmind upload\n");
		
		Maxmind mxmind = new Maxmind();
		
		mxmind.csv2inf();
		mxmind.uploadInf();
	}	
}
