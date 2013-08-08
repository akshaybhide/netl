
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.slicerep.SliUtil.*;

public class Stage2MainPull
{	
	private SortedSet<DimCode> _dimSet;
	private SortedSet<IntFact> _intSet;
	private SortedSet<DblFact> _dblSet;
	private SortedSet<String> _targColSet;
	
	public static final int HOUR_BEHIND_LAG = 3;
	
	private AggType _aType;
	
	private int _updateCount = 0;

	// private String _targDayCode;
	// private Integer _targHour;	
	
	private SimpleMail _logMail;
	
	private List<WhereBatch> _whereList = Util.vector();
		
	private String _overrideDstName;
	
	private boolean _doDelete = true;
	
	private static WhereBatch getDefaultWhereBatch()
	{
		return getDefaultWhereBatch(new GregorianCalendar());
	}
	
	static WhereBatch getDefaultWhereBatch(Calendar cal)
	{
		Pair<String, Integer> datehour = getDateHourPair(cal);
		String targday = datehour._1;
		int targhour = datehour._2;		
		
		WhereBatch whb = new WhereBatch();
		whb.addFrag(Util.sprintf(" ((id_date < '%s') OR (id_date = '%s' AND id_hour < %d ))", targday, targday, targhour));
		return whb;
	}
	
	static List<WhereBatch> getCampTimeBatchList(Calendar cal)
	{
		List<WhereBatch> wblist = Util.vector();
		for(int i = 0; i < 16; i++)
		{
			WhereBatch wb = getDefaultWhereBatch(cal);	
			String campfrag = getCampListWhereFrag(i);
			wb.addFrag(campfrag);
			wblist.add(wb);
		}
		return wblist;		
	}
	
	
	public static WhereBatch getSpecificDateWhereBatch(String daycode, int hour)
	{
		WhereBatch wb = new WhereBatch();
		wb.addFrag(Util.sprintf(" ID_DATE = '%s' ", daycode));
		wb.addFrag(Util.sprintf(" ID_HOUR = %d ", hour));
		return wb;
	}
	
	public static WhereBatch getCampaignWhereBatch(int campid)
	{
		WhereBatch wb = new WhereBatch();
		wb.addFrag(Util.sprintf(" ID_CAMPAIGN = %d ", campid));
		return wb;		
	}
	
	public static String getCampListWhereFrag(int packid)
	{
		List<Integer> camplist = SliUtil.getCampListForPack(6000, packid);
		return Util.sprintf(" ID_CAMPAIGN in ( %s ) ", Util.join(camplist, ","));
	}
		
	private static Pair<String, Integer> getDateHourPair(Calendar targcal)
	{
		String longdaycode = TimeUtil.cal2LongDayCode(targcal);
		String[] day_time = longdaycode.split(" ");
		String daycode = day_time[0];
		
		Integer hour = Integer.valueOf(day_time[1].split(":")[0]);
		hour -= HOUR_BEHIND_LAG;
		if(hour < 0)
		{
			daycode = TimeUtil.dayBefore(daycode);	
			hour += 24;
		}
		
		return Pair.build(daycode, hour);
	}
	
	public Stage2MainPull(AggType at)
	{
		this(at, new SimpleMail("GIMP"));	
	}
	
	public Stage2MainPull(AggType at, SimpleMail lmail)
	{
		this(at, lmail, getDefaultWhereBatch());
	}
	
	public Stage2MainPull(AggType at, SimpleMail lmail, WhereBatch whbatch)
	{
		this(at, lmail, Arrays.asList(new WhereBatch[] { whbatch }));
	}	
	
	public Stage2MainPull(AggType at, SimpleMail lmail, List<WhereBatch> whlist)
	{
		_logMail = lmail;
		Util.massert(_logMail != null, "Cannot use with null logmail");		
		
		_aType = at;
		_dimSet = DatabaseBridge.getDimSet(_aType, DbTarget.internal);
		_intSet = DatabaseBridge.getIntFactSet(_aType, DbTarget.internal);
		_dblSet = DatabaseBridge.getDblFactSet(_aType, DbTarget.internal);
		
		_targColSet = new TreeSet<String>(DatabaseBridge.getTableColNames(_aType, DbTarget.internal));
		
		_whereList.addAll(whlist);
	}		
	
	private void initColSetInfo()
	{
		_targColSet = new TreeSet<String>(DbUtil.getColNameSet(getDstTableName(), new DatabaseBridge(DbTarget.internal)));
		
		Util.pf("Targ col set is %s\n", _targColSet);
	}
	
	// TODO: this thing really sucks,
	// it is intended to support uploads to multiple DBs simultaneously
	public void runAllUpdates()
	{
		runAllUpdates(DbTarget.internal);	
	}
	
	public void runAllUpdates(DbTarget onetarg)
	{
		for(int whind : Util.range(_whereList.size()))
		{
			runUpdate(whind, onetarg);	
		}
	}
	
	public void runUpdate(int whind, DbTarget onetarg)
	{
		String onewhere = _whereList.get(whind).getWhereClause();
		// _logMail.pf("Where clause is %s\n", onewhere);
				
		Connection conn = null;
		try {
			conn = (new DatabaseBridge(onetarg)).createConnection();
			conn.setAutoCommit(false);
			
			double startup = Util.curtime();
			
			String fullsql = getFullSql(whind);
			_logMail.pf("Full sql is %s\n", fullsql);
			
			PreparedStatement pstmt = conn.prepareStatement(fullsql);
			int rowsin = pstmt.executeUpdate();

			int rowdel = 0;
			if(_doDelete)
			{
				String delsql = getDeleteSql(whind);
				PreparedStatement delstmt = conn.prepareStatement(delsql);
				rowdel = delstmt.executeUpdate();
			}
						
			conn.commit();
			
			_logMail.pf("S2MP dbtarg:%s, inserted %d rows, deleted %d rows, took %.03f\n", onetarg, rowsin, rowdel, (Util.curtime()-startup)/1000);	
			
			_updateCount++;
			
		} catch (SQLException sqlex) {
			
			sqlex.printStackTrace();
			
			SimpleMail exmail = new SimpleMail("ERROR in Stage2MainPull");
			exmail.addExceptionData(sqlex);
			exmail.send2admin();
			
			
		} finally {
			
			if(conn != null)
			{ 
				try { conn.close(); }
				catch (Exception ex) { }
			}
		}
	}
	
	public int getUpdateCount()
	{
		return _updateCount;
	}	
	
	private String getSrcTableName()
	{
		return DatabaseBridge.getStageTableName(DbTarget.internal, _aType);
	}
	
	public void setTestMode(String testtabname)
	{
		_overrideDstName = testtabname;	
		_doDelete = false;
		initColSetInfo();
	}
	
	private String getDstTableName()
	{
		//return "__" + SliDatabase.getAggTableName(_aType) + "_tmp";
		return (_overrideDstName == null ? SliDatabase.getAggTableName(_aType) : _overrideDstName);
	}
	
	private String getDeleteSql(int whind)
	{
		return Util.sprintf("DELETE FROM %s %s ", getSrcTableName(), _whereList.get(whind).getWhereClause());	
	}
	
	private String getFullSql(int whind)
	{
		return getInsertSql() + "\n" + getSelectSql(whind);	
	}
	
	private String getInsertSql()
	{
		List<String> insertlist = Util.vector();
		for(DimCode onedim : _dimSet)
		{
			// This could be something like "unassign_int_2"
			insertlist.add(onedim.getBaseTableColName());
			// insertlist.add("id_" + onedim.toString());	
		}
		insertlist.add("ENTRY_DATE");
		
		if(_targColSet.contains("HAS_CC"))
			{ insertlist.add("HAS_CC"); }
		
		if(_targColSet.contains("RAND99"))
			{ insertlist.add("RAND99"); }	
		
		for(IntFact ifact : _intSet)
		{
			insertlist.add(Util.sprintf("num_%s", ifact.toString())); 
		}
		
		for(DblFact dfact : _dblSet)
		{
			insertlist.add(Util.sprintf("imp_%s", dfact.toString())); 
		}
		
		
		return Util.sprintf("INSERT INTO %s (%s) ", 
			getDstTableName(), Util.join(insertlist, ","));
	}
	
	// TODO: the select/insert statements should be paired together in a map
	private String getSelectSql(int whind)
	{
		String whclause = _whereList.get(whind).getWhereClause();
		
		List<String> basiclist = Util.vector();
		List<String> grouplist = Util.vector();
		
		for(DimCode onedim : _dimSet)
		{
			Util.massert(onedim != DimCode.quarter, "DimCode quarter not allowed in main table");
			
			String dimstr = "id_" + onedim.toString();
			basiclist.add(dimstr); 
			grouplist.add(dimstr);
		}
		
		basiclist.add("ENTRY_DATE");
		grouplist.add("ENTRY_DATE");
		
		if(_targColSet.contains("HAS_CC"))
			{ basiclist.add(" IF(sum(num_clicks)+sum(num_conversions) > 0, 1, 0) "); }
		
		if(_targColSet.contains("RAND99"))
			{ basiclist.add(" FLOOR(RAND()*(100 - 1e-14)) "); }	

		List<String> factlist = Util.vector();
		for(IntFact ifact : _intSet)
		{
			factlist.add(Util.sprintf("sum(num_%s)", ifact.toString())); 
		}
		
		for(DblFact dfact : _dblSet)
		{
			factlist.add(Util.sprintf("sum(imp_%s)", dfact.toString())); 
		}
	
		
		return Util.sprintf("SELECT %s, %s FROM %s %s GROUP BY %s", 
			Util.join(basiclist, ","), Util.join(factlist, ","), getSrcTableName(), whclause, Util.join(grouplist, ","));
	}
	
	
	public static class WhereBatch
	{
		List<String> _fraglist = Util.vector();
		
		public WhereBatch()
		{
		}
		
		public WhereBatch(String wherestr)
		{
			Util.massert(wherestr.toLowerCase().indexOf("where") == -1, "Do not include base where in arg");
			// Util.massert(wherestr.toLowerCase().indexOf("and") == -1, ", Do not include and clauses");
			_fraglist.add(wherestr);
		}
		
		public String getWhereClause()
		{
			return Util.sprintf(" WHERE %s ", Util.join(_fraglist, " AND "));	
		}
		
		public void addFrag(String frag)
		{
			_fraglist.add(frag);	
		}
		
		public int numFrags()
		{
			return _fraglist.size();	
		}
	}
	
	public static void main(String[] args)
	{
		while(true)
		{
			SimpleMail logmail = new SimpleMail("Stage2Main Pull Report for " + TimeUtil.getTodayCode());
			TreeSet<Pair<String, Integer>> timeset = getDateHourSet(DbTarget.internal);
			int numupdates = 0;
			
			while(timeset.size() > 6)
			{
				Pair<String, Integer> dhpair = timeset.pollFirst();
				logmail.pf("Going to run S2MP for pair %s\n", dhpair);
				
				WhereBatch whbatch = getSpecificDateWhereBatch(dhpair._1, dhpair._2);
				
				for(AggType atype : AggType.values())
				{
					Stage2MainPull s2mp = new Stage2MainPull(atype, logmail, whbatch);
					
					s2mp.runAllUpdates(DbTarget.internal);
					// s2mp.runAllUpdates(DbTarget.new_internal);
				}
				
				numupdates++;
			}
			
			logmail.pf("Ran %d updates\n", numupdates);
			logmail.send2admin();
			
			for(int i : Util.range(60))
			{
				Util.pf("z");
				try {  Thread.sleep(60*1000); }
				catch (Exception ex) {}
			}
		}
	}
	
	private static TreeSet<Pair<String, Integer>> getDateHourSet(DbTarget dbtarg)
	{
		TreeSet<Pair<String, Integer>> timeset = Util.treeset();
		String sql = "SELECT id_date, id_hour FROM fast_domain_stage GROUP BY id_date, id_hour";
		List<Pair<java.sql.Date, Integer>> timelist = DbUtil.execSqlQueryPair(sql, new DatabaseBridge(dbtarg));
		
		for(Pair<java.sql.Date, Integer> onepair : timelist)
		{
			timeset.add(Pair.build(onepair._1.toString(), onepair._2));	
		}
		
		return timeset;		
	}
		
	
	
	private static TreeMap<Pair<String, Integer>, Integer> getDateHourCountMap(DbTarget dbtarg)
	{
		TreeMap<Pair<String, Integer>, Integer> countmap = Util.treemap();
		
		try {
			
			Connection conn = (new DatabaseBridge(dbtarg)).createConnection();
			String sql = "SELECT id_date, id_hour, count(*) as rcount FROM fast_domain_stage GROUP by id_date, id_hour";
			PreparedStatement pstmt = conn.prepareStatement(sql); 
			
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				String daycode = rset.getString("id_date");
				int hour = rset.getInt("id_hour");
				int count = rset.getInt("rcount");
				countmap.put(Pair.build(daycode, hour), count);
			}	
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);	
			
		}
		
		return countmap;
	}
	
}
