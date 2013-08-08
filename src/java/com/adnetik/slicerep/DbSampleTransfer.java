
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.slicerep.SliUtil.*;

public class DbSampleTransfer 
{
	// As of 2012-09-15, max(id) from campaign = 2503, this gives a safety net
	public static final int MAX_CAMP_ID = 3000;
	AggType _aggType;
	
	Set<String> _overlapSet;
			
	WhereClause _whGen;
 
	public DbSampleTransfer(AggType at, WhereClause wc)
	{
		_aggType = at;
		Util.massert(_aggType == AggType.ad_domain, "Only ready for ad_domain");
		
		// This has to be done before doing getColOverlap
		createCopyViews();		
		
		_overlapSet = getCheckColOverlap();
			
		_whGen = wc;
	}
	
	private TreeSet<String> getCheckColOverlap()
	{
		TreeSet<String> dstset = getColNameSet(getDstName());
		TreeSet<String> srcset = getColNameSet(getSrcName());
		
		Util.massert(dstset.equals(srcset), "DST set \n %s \n %s", dstset, srcset);
		
		Util.pf("DST first=%s last=%s size=%d", dstset.first(), dstset.last(), dstset.size());
		Util.pf("DST first=%s last=%s size=%d", srcset.first(), srcset.last(), srcset.size());
		
		return dstset;
	}
	
	private TreeSet<String> getColNameSet(String tabname)
	{
		String sql = "DESCRIBE " + tabname;
		List<String> collist = DbUtil.execSqlQuery(sql, new DatabaseBridge(DbTarget.internal));
		return new TreeSet<String>(collist);
		
	}
	
	private String getSrcName()
	{
		return getSrcName(_aggType);
	}
	
	private static String getSrcName(AggType atype)
	{
		return Util.sprintf("__v_%s_sample", DatabaseBridge.getAggTableName(DbTarget.internal, atype));	
		
	}
	
	private String getBaseName()
	{
		return getBaseName(_aggType);	
	}
	
	private static String getBaseName(AggType atype)
	{
		return Util.sprintf("%s", DatabaseBridge.getAggTableName(DbTarget.internal, atype));
		// return Util.sprintf("%s__old", DatabaseBridge.getAggTableName(DbTarget.internal, atype));
	}
	
	private String getDstName()
	{
		return getDstName(_aggType);
	}
	
	private static String getDstName(AggType atype)
	{
		// return Util.sprintf("%s__new", DatabaseBridge.getAggTableName(DbTarget.internal, atype));
		// return Util.sprintf("%s__datecopy", DatabaseBridge.getAggTableName(DbTarget.internal, atype));
		return Util.sprintf("%s__new", DatabaseBridge.getAggTableName(DbTarget.internal, atype));
	}
	
	private String getInsertSql(String whclause)
	{
		String fieldcsv = Util.join(_overlapSet, ",");
		
		return Util.sprintf(" INSERT INTO %s ( %s )\n SELECT %s FROM %s %s", 
			getDstName(), fieldcsv, fieldcsv, getSrcName(), whclause);
	}
	
	private void checkTargetEmpty()
	{
		String sql = Util.sprintf("SELECT id_campaign FROM %s LIMIT 10", getDstName());
		List<Integer> idlist = DbUtil.execSqlQuery(sql, new DatabaseBridge(DbTarget.internal));
		
		Util.massert(idlist.isEmpty(), "Target table is not empty, aborting");
		Util.pf("Okay, target table %s is empty\n", getDstName());
	}
	
	private void runUpdate()
	{
		double totrows = 0.0D;
		
		for(int opid : Util.range(_whGen.numOps()))
		{
			String whclause = _whGen.getWhereClause(opid);
			String opname = _whGen.getOpName(opid);			
			String insql = getInsertSql(whclause);
			
			try {
				Connection conn = (new DatabaseBridge(DbTarget.internal)).createConnection();
				
				int rows = DbUtil.execWithTime(insql, opname, conn, _whGen._logMail);
				
				totrows += rows;	
				
				if((opid % 5) == 4)
				{
					_whGen._logMail.pf("Done with %d operations, %.01f rows, average %.03f rows/part\n", 
						(opid+1), totrows, totrows/(opid+1));
				}	
				
				conn.close();
				
			} catch (SQLException sqlex) {
				
				_whGen._logMail.pf("SQL exception on opid %d", opid);
				_whGen._logMail.pf("WHERE clause is %s", whclause);
				_whGen._logMail.pf("Message is %s", sqlex.getMessage());
			}
		
			_whGen.finishedOp(opid);
		}
	}
	
	private void createCopyViews()
	{
		createCopyViews(_aggType);
	}
	
	static void createCopyViews(AggType atype)
	{
		String cviewsql = Util.sprintf("CREATE OR REPLACE VIEW %s AS SELECT *, IF(num_clicks+num_conversions > 0, 1, 0) as HAS_CC, floor(rand() * 100) as RAND99 from %s",
			getSrcName(atype), getBaseName(atype));
		
		Util.pf("Create view is \n %s\n", cviewsql);
		
		DbUtil.execWithTime(cviewsql, "created view for " + atype, new DatabaseBridge(DbTarget.internal));
	}	
	
	private static abstract class WhereClause
	{
		
		SimpleMail _logMail = new SimpleMail("DB transition report");		
		
		public abstract int numOps();	
		
		public abstract String getWhereClause(int opid); 
		
		public String getOpName(int opid)
		{
			return Util.sprintf("OPERATION %d", opid);
		}
		
		public void finishedOp(int opid) 
		{
			if(opid == numOps()-1)
			{
				_logMail.send2admin();	
			}
		}
	}
	
	private static class CampaignBased extends WhereClause
	{
		List<Integer> _partList = Util.vector();
		
		public CampaignBased()
		{
			for(int pid = 0; pid < 1024; pid++)
				{ _partList.add(pid); }
		}
		
		public int numOps()
		{
			return _partList.size();	
		}
		
		public String getWhereClause(int partid)
		{
			List<String> camplist = Util.vector();
			
			for(int x = 0; x < 4; x++)
			{
				camplist.add((partid + x*1024)+"");
			}
			
			String whclause = Util.sprintf(" WHERE ID_CAMPAIGN IN ( %s ) %s ", Util.join(camplist, ","), getEntryDateWhere());
			
			return whclause;
		}
		
		private String getEntryDateWhere()
		{
			return "";
			
		}
	}
		
	private static class DateBased extends WhereClause
	{
		List<String> _dayList = Util.vector();
		
		public DateBased()
		{
			_dayList = TimeUtil.getDateRange("2012-09-12", 100);
			Collections.reverse(_dayList);
		}
		
		public String getWhereClause(int opid)
		{
			String daycode = _dayList.get(opid);
			return Util.sprintf(" WHERE ENTRY_DATE = '%s' ", daycode);
		}
		
		public int numOps()
		{
			return _dayList.size();	
			
		}
	}
	
	private static class DateAndCampaign extends WhereClause
	{
		private static final int NUM_CAMP_PACK = 16;
		
		List<Pair<String, Integer>> _dayCampList = Util.vector();
		
		public DateAndCampaign()
		{
			TreeSet<String> dayset = new TreeSet<String>(Collections.reverseOrder());
			dayset.addAll(TimeUtil.getDateRange("2012-06-29", "2012-10-01"));
					
			for(String oneday : dayset)
			{
				for(int i : Util.range(NUM_CAMP_PACK))
				{
					_dayCampList.add(Pair.build(oneday, i));
				}
			}
			
			resetLogMail(0);
		}
		
		public String getWhereClause(int opid)
		{
			String daycode = _dayCampList.get(opid)._1;
			int packid = _dayCampList.get(opid)._2;
			
			String campwhere = Util.sprintf(" id_campaign IN ( %S ) ", Util.join(getCampListForPack(packid), ","));
			
			return Util.sprintf(" WHERE ENTRY_DATE = '%s' AND %s ", daycode , campwhere);
		}
		
		@Override
		public String getOpName(int opid)
		{
			Pair<String, Integer> daypack = _dayCampList.get(opid);
			return Util.sprintf("DATE=%s, PACK=%d/%d", daypack._1, daypack._2, NUM_CAMP_PACK);
		}		
		
		private List<Integer> getCampListForPack(int packid)
		{
			List<Integer> packlist = Util.vector();
			for(int campid = packid; campid < MAX_CAMP_ID; campid += NUM_CAMP_PACK)
			{
				packlist.add(campid);				
			}
			return packlist;
		}
		
		
		public int numOps()
		{
			return _dayCampList.size();
		}		
		
		private void resetLogMail(int opid)
		{
			if(opid < _dayCampList.size())
			{
				String newday = _dayCampList.get(opid)._1;
				_logMail = new SimpleMail("DateCampaign based DB transition for " + newday);
				_logMail.pf("Reset log mail for %s", newday);
			}			
		}
		
		@Override
		public void finishedOp(int opid)
		{
			if((opid % NUM_CAMP_PACK) == NUM_CAMP_PACK-1)
			{
				_logMail.send2admin();
				resetLogMail(opid);
			}
		}
	}
	
	public static void main(String[] args)
	{
		// DbTransition dbtrans = new DbTransition(AggType.ad_general);
		// Util.pf("Campaign where is %s\n", dbtrans.getCampaignInClause(3));
		
		/// WhereClause whcls = new DateBased();
		WhereClause whcls = new DateAndCampaign();
		
		AggType atype = AggType.ad_domain;
		
		
		DbSampleTransfer dbtrans = new DbSampleTransfer(atype, whcls);
		dbtrans.checkTargetEmpty();		

		Util.pf("Going to run DbTransition for : \nwhere_clause: %s\ndest table: %s\n",
			whcls.getWhereClause(0), getDstName(atype));		
		
		Util.pf("Overlap set is %s\n", Util.join(dbtrans._overlapSet, ","));
		
		Util.pf("ONE PARTITION INSERT sql is \n%s\n", dbtrans.getInsertSql(whcls.getWhereClause(0)));
		
		if(!Util.checkOkay("Okay to proceed?"))
		{ 
			Util.pf("Aborting...");
			return; 
		}
		
		dbtrans.runUpdate();
	}
	
}
