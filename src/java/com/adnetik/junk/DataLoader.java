
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.slicerep.SliUtil.*;

public class DataLoader
{
	/*
	public static final String TEMP_INF = "temp_inf.txt";
	
	public DataLoader()
	{
		
		
	}
	
	// Transform a log file into an 
	private void log2inf(String filepath) throws IOException
	{
		BufferedWriter bwrite = FileUtils.getWriter(TEMP_INF);
		Map<String, Integer> datamap = Util.treemap();
		
		PathInfo pinfo = new PathInfo(filepath);
		BufferedReader bread = Util.getGzipReader(filepath);
		int lcount = 0;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
			if(ble == null)
				{ continue; }
			
			datamap.clear();
			ble2map(ble, datamap);
			// Util.pf("Data map is %s\n", datamap);
			map2inf(datamap, bwrite);
			lcount++;
			
			if((lcount % 100) == 0)
			{
				Util.pf("Done with line %d\n", lcount);	
				
			}
		}
		
		bread.close();	
		bwrite.close();
		
		int uprows = SliDatabase.loadFileToMain(new File(TEMP_INF), CatManager.getSing().getMainFactCols());
		Util.pf("Uploaded %d rows from %d file lines\n", lcount, uprows);
	}
	
	private void map2inf(Map<String, Integer> datamap, Writer towrite) throws IOException
	{
		List<String> inflist = Util.vector();
		
		for(String colname : CatManager.getSing().getMainFactCols())
		{
			Util.massert(datamap.containsKey(colname), "Error, colname %s not found in datamap", colname);
			
			inflist.add(datamap.get(colname)+"");
		}
		
		towrite.write(Util.join(inflist, "\t"));
		towrite.write("\n");
	}
	
	private void ble2map(BidLogEntry ble, Map<String, Integer> datamap)
	{
		
		for(DimCode dcode : DimCode.values())
		{
			if(dcode.doLookup)
			{
				String relval = ble.getField(dcode.toString());
				int id = CatManager.getSing().lookupCatMap(dcode, relval);
				datamap.put("dim_" + dcode.toString(), id);
			} else {
				int straitval = ble.getIntField(dcode.toString());
				datamap.put("dim_" + dcode.toString(), 	straitval);
			}
		}
		
		datamap.put("entry_date", 20120514);
		datamap.put("dim_date", 20120514);
		datamap.put("dim_hour", 14);
	}
	
	public static void main(String[] args) throws Exception
	{
		DataLoader dload = new DataLoader();
		CatManager.getSing().readCatData();		
		
		Collection<String> pathlist = Util.getNfsPathSizeMap(ExcName.rtb, LogType.imp, "2012-05-24").keySet();
		
		for(String onepath : pathlist)
		{
			dload.log2inf(onepath);
			break; 	
		}
	}
	*/
}
