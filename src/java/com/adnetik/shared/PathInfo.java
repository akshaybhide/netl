
package com.adnetik.shared;

import java.util.*;
import java.io.*;
import java.util.regex.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

public class PathInfo
{
	public LogType pType;
	public LogVersion pVers;
	
	public DataCenter pCenter;
	public ExcName pExc;
	
	private String _fPath;
	private String _entryDate ="";
	private PathInfo() {}
	
	public PathInfo(String fullpath)
	{
		_fPath = fullpath;
		
		String simpname = Util.lastToken(fullpath, "/");
		
		//Util.pf("\nSimpname is %s, toks is %d", simpname, simpname.split("\\.").length);
		
		String[] subtoks = simpname.split("\\.");
		
		// logtype_version
		// Note: this is going to work because the LogVersion tag is EXACTLY three characters long
		// v20, v21, etc
		{
			String ltv = subtoks[2];
			int flen = ltv.length();
			pVers = LogVersion.valueOf(ltv.substring(flen-3, flen));
			pType = LogType.valueOf(ltv.substring(0, flen-4));
		}
		
		// datacenter
		{
			String targs = subtoks[3];
			
			pExc = lookupFromString(targs);
			
			for(DataCenter dc : DataCenter.values())
			{
				if(targs.indexOf(dc.toString()) > -1)
					{ pCenter = dc; }
			}
		}
		
		{
			Pattern pattern = Pattern.compile("[2][0-2][0-9][0-9]-[0-1][0-9]-[0-9][0-9]");
			Matcher matcher = pattern.matcher(getActualPath());
			// Check all occurrences
			if(matcher.find()) {
				_entryDate = getActualPath().substring(matcher.start(), matcher.end());
			}
		}
		
		// Calendar
		// pCalendar = Util.calFromNfsPath(fullpath);
	}
	
	public String getDate() 
	{
		return _entryDate;
	}
	
	public String getEntryDate()
	{
		return _entryDate;	
	}
	
	// Path used to create this PathInfo
	public String getActualPath()
	{
		return _fPath;		
	}
	
	public String toString()
	{
		return Util.sprintf("logtype=%s\tlogversion=%s\tdatacenter=%s\texcname=%s", pType, pVers, pCenter, pExc);	
	}
	
	public static PathInfo fromLzoPath(String lzopath)
	{
		// Util.massert(lzopath.endsWith(".lzo"), "Invalid LZO path %s", lzopath);
		
		int dataind = lzopath.indexOf("/data");
		int lzoind = lzopath.indexOf(".lzo");
		
		// Util.pf("Data ind is %d, lzo is %d", dataind, lzoind);
		
		Util.massert(dataind != -1 && lzoind != -1, "Bad LZO Path %s", lzopath);
		
		// eg /data/imp/yahoo_2012-09-27_v19.lzo.index
		String relstr = lzopath.substring(dataind+1, lzoind);
		String[] d_lt_fname = relstr.split("/");
		
		// Util.pf("rel str is %s\n", relstr);
		
		Util.massert(d_lt_fname[0].equals("data"), "Bad LZO Path %s", lzopath);
		
		PathInfo pinf = new PathInfo();
		pinf.pType = LogType.valueOf(d_lt_fname[1]);
		
		String[] ex_dc_lv = d_lt_fname[2].split("_");
		pinf.pVers = LogVersion.valueOf(ex_dc_lv[2]);
		
		pinf.pExc = lookupFromString(ex_dc_lv[0]);
		
		pinf._fPath = lzopath;
		return pinf;
	}
	
	private static ExcName lookupFromString(String lookup)
	{
		for(ExcName oneex : ExcName.values())
		{
			for(String onename : oneex.getNameSet())
			{
				if(lookup.startsWith(onename))
					{ return oneex; }
			}
		}
		
		throw new RuntimeException("Could not find exchange name code for lookup code " + lookup);
	}
	
	public static void main(String[] args)
	{
		PathInfo p =  new PathInfo("/mnt/adnetik/adnetik-uservervillage/dbh/userver_log/imp/2013-03-04/2013-03-04-23-45-01.EST.imp_v21.dbh-rtb-california2_73305.log.gz");
		System.out.println("date = " + p.getDate());
		List<String> datelist = TimeUtil.getDateRange("2012-10-15");
		Collections.shuffle(datelist);
		
		LogType[] ltypes = new LogType[] { LogType.no_bid_all, LogType.bid_all, LogType.imp, LogType.click, LogType.conversion };
		
		
		for(String onedate : datelist)
		{
			// for(ExcName oneexc : new ExcName[] { ExcName.improvedigital, ExcName.rtb })
			for(ExcName oneexc : ExcName.values())			
			{
				for(LogType onetype : ltypes)
				{
					List<String> pathlist = Util.getNfsLogPaths(oneexc, onetype, onedate);
					
					if(pathlist == null)
					{ 
						pathlist = Util.vector();	
					}
					
					
					
					for(String onepath : pathlist)
					{
						PathInfo pinf = new PathInfo(onepath);
						Util.massert(pinf.pExc == oneexc, "Expected %s, found %s for file %s",
							oneexc, pinf.pExc, onepath);
					}
					
					Util.pf(" %d paths checked okay for exc=%s, log=%s, date=%s\n",
						pathlist.size(), oneexc, onetype, onedate);					
				}
			}
		}
	}
}

