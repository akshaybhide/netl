
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.bm_etl.BmUtil.*;

public abstract class PathInfoManager
{
	
	public abstract List<String> getPathsForQCode(String dc, QuarterCode qc, LogType[] touse); 

	
	public static class ArmitaImpl extends PathInfoManager
	{
		public List<String> getPathsForQCode(String daycode, QuarterCode qc, LogType[] touse)
		{
			List<String> plist = Util.vector();
			
			
			if(qc.getHour() == 0)
			{
				// Do something smart	
				throw new RuntimeException("nyi");

			} else {

				// java.util.regex.Pattern dcpatt = java.util.regex.Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d");
				
				
			}
			
			return plist;
		}
	}
	
	public static class DumbDanImpl extends PathInfoManager
	{
		
		public List<String> getPathsForQCode(String daycode, QuarterCode targcode, LogType[] touse)
		{
			List<String> plist = Util.vector();
			Map<QuarterCode, Integer> qcalmap = QuarterCode.getQuarterIntMap();
			
			// TODO expand this
			Map<String, Calendar> bigmap = Util.treemap();
			for(ExcName oneex : ExcName.values())
			{
				for(LogType onetype : touse)
				{
					Map<String, Calendar> nfsmap = Util.getNfsLogPathCalMap(oneex, onetype, daycode);
					if(nfsmap == null)
						{ continue; }
					
					bigmap.putAll(nfsmap);
				}
			}
			
			for(String onepath : bigmap.keySet())
			{
				Calendar pathcal = bigmap.get(onepath);
				String timestamp = Util.cal2TimeStamp(pathcal);
				QuarterCode qcode = QuarterCode.nearestQCode(qcalmap, timestamp);
				
				if(targcode.equals(qcode))
					{ plist.add(onepath); }
			}		
			
			return plist;
		}		
	}
	
	public static void main(String[] args)
	{
		LogType[] touse = new LogType[] { LogType.bid_all, LogType.imp, LogType.click, LogType.conversion };
		
		PathInfoManager pim = new DumbDanImpl();
		
		QuarterCode qcode = new QuarterCode((short) 14, (short) 2);
		
		List<String> plist = pim.getPathsForQCode("2012-06-15", qcode, touse);
		
		for(String onepath : plist)
		{
			Util.pf("%s\n", onepath);	
			
		}
		
		Util.pf("Total number of paths is %d\n", plist.size());
		
	}
}
