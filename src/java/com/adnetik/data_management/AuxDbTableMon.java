
package com.adnetik.data_management;

import java.io.*;
import java.util.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;


// Monitor some external database tables
public class AuxDbTableMon
{	
	private static final int DAY_QUERY_SIZE = 30;
	
	public enum MonTarget { 
		
		pixparam(DbTarget.external, "pixparam", "v__combined", 100000L),
		party3_icc(DbTarget.internal, "thirdparty", "3p_icc", 100000L, "daycode"),
		exusage(DbTarget.internal, "thirdparty", "ex_usage_main", 10000L) ;
		
		DbTarget dbTarg;
		String dbName; 
		String tabName; 
		String dateField;
		Long warnCutoff;
		
		MonTarget(DbTarget dbt, String dbname, String tabname, Long cut, String df)
		{
			dbTarg = dbt;
			dbName = dbname;
			tabName = tabname;
			dateField = df;
			warnCutoff = cut;
		}		
		
		MonTarget(DbTarget dbt, String dbname, String tname, Long cutoff)
		{
			this(dbt, dbname, tname, cutoff, "nfsdate");
		}
		
	};
	
	private SimpleMail _logMail;
	private MonTarget _monTarg;
	
	public static void main(String[] args) throws Exception
	{
		SimpleMail logmail = new SimpleMail("AuxDbTableMon for " +  TimeUtil.getYesterdayCode());
		
		for(MonTarget mt : MonTarget.values())
		{
			AuxDbTableMon adtm = new AuxDbTableMon(mt, logmail);
			adtm.runQuery();
		}
		
		
		logmail.send2AdminList(AdminEmail.burfoot, AdminEmail.sekhark);
	}
	
	public AuxDbTableMon(MonTarget mt, SimpleMail sm)
	{
		_logMail = sm;
		_monTarg = mt;
	}
	
	private static String getStartQueryDate()
	{
		return TimeUtil.nDaysBefore(TimeUtil.getYesterdayCode(), DAY_QUERY_SIZE+2);	
	}
	
	private static SortedSet<String> getTargDaySet()
	{
		String sday = getStartQueryDate();
		
		SortedSet<String> dayset = Util.treeset();
		while(dayset.size() < DAY_QUERY_SIZE)
		{
			dayset.add(sday);
			sday = TimeUtil.dayAfter(sday);
		}
		return dayset;
	}
	
	private void runQuery()
	{
		SortedMap<String, Long> countmap = getCountMap(_monTarg);

		checkCountMap(countmap);		
	}
	
	private void checkCountMap(SortedMap<String, Long> countmap)
	{
		int errdays = 0;
		
		for(String oneday : getTargDaySet())
		{
			long thecount = countmap.containsKey(oneday) ? countmap.get(oneday) : 0;
			
			if(thecount < _monTarg.warnCutoff)
			{
				_logMail.pf("Warning, found only %d counts for %s, mtarg %s\n", 
					thecount, oneday, _monTarg);
				
				errdays++;
			}
		}
		
		_logMail.pf("Finished query for %s, found %d error days\n", _monTarg, errdays);
	}
	
	private static SortedMap<String, Long> getCountMap(MonTarget mtarg)
	{
		Map<java.sql.Date, Long> countmap = Util.treemap();	
		DbUtil.popMapFromQuery(countmap, genSql(mtarg), new DatabaseBridge(mtarg.dbTarg));
		return date2strMap(countmap);
	}
	
	private static SortedMap<String, Long> date2strMap(Map<java.sql.Date, Long> datemap)
	{
		SortedMap<String, Long> strmap = Util.treemap();
		
		for(java.sql.Date oneday : datemap.keySet())
		{
			strmap.put(oneday.toString(), datemap.get(oneday));
		}
		
		return strmap;
	}
	
	private static String genSql(MonTarget mtarg)
	{
		return genSql(mtarg.dbName, mtarg.tabName, mtarg.dateField);
		
		
	}
	
	private static String genSql(String dbname, String tabname, String dfield)
	{
		SortedSet<String> targset = getTargDaySet();
		
		return Util.sprintf("SELECT %s, count(*) FROM %s.%s WHERE '%s' <= %s and %s <= '%s' GROUP BY %s",
			dfield, dbname, tabname, targset.first(), dfield, dfield, targset.last(), dfield);
	}
}
