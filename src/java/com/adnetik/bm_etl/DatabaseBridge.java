package com.adnetik.bm_etl;

import java.util.*;
import java.sql.*;
import java.io.*; 

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*; 
import com.adnetik.shared.DbUtil.*; 

import com.adnetik.shared.BidLogEntry.*; 
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

/** 
 * DatabaseBridge
 */ 
public class DatabaseBridge implements DbUtil.ConnectionSource
{	
	public static int DB_PARTITIONS = 512;

	static Random RANDOM_CHECK = new Random();
	
	//public static final String DB_IP_ADD = "174.140.150.194";
	public static final String INTERNAL_DB_MACHINE = "thorin-internal.digilant.com";
	
	public static final String ARCHIVE_DB_MACHINE = "thorin-internal-replication.digilant.com";
	
	public static final String NEW_THORIN_ADDR = "anacluster09.adnetik.iponweb.net";
	
	// public static final String INTERNAL_DB_MACHINE = "174.140.132.153";

	public static final String EXTERNAL_DB_MACHINE = DbUtil.ThorinExternalConn.HOST_NAME;

	public static final String EXTERNAL_STAGING_MACHINE = "thorin-staging.adnetik.com";

	public static final int NUM_SUB_PART = 10;
	
	private DbTarget _dbTarg;
	
	// What the hell is this?
	private static Set<DimCode> _NAMEONLY = Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<DimCode,Boolean>());
	
	//new ConcurrentHashMap<DimCode, Boolean>();		
	
	static {
		// _NAMEONLY.add(DimCode.exchange);
		_NAMEONLY.add(DimCode.visibility);
		_NAMEONLY.add(DimCode.size);
		_NAMEONLY.add(DimCode.browser);
		_NAMEONLY.add(DimCode.gender);
		_NAMEONLY.add(DimCode.domain);
		// _NAMEONLY.add(DimCode.lineitem);
	}	
	
	// TODO: getHostName should be a method on DbTarget object
	public static String[] getIpDbName(DbTarget dbtarg)
	{
		if(dbtarg == DbTarget.internal)
		{
			return new String[] { INTERNAL_DB_MACHINE, "fastetl" } ;
			
		} else if(dbtarg == DbTarget.replication) { 
		
			return new String[] { ARCHIVE_DB_MACHINE, "fastetl" };
		
		} else if(dbtarg == DbTarget.ext_staging) { 
		
			return new String[] { EXTERNAL_STAGING_MACHINE, "bm_etl" };
			
		} else if(dbtarg == DbTarget.external) {
			
			return new String[] { EXTERNAL_DB_MACHINE, "bm_etl" };
			
		} else if(dbtarg == DbTarget.new_internal) {
			
			return new String[] { NEW_THORIN_ADDR, "fastetl" };
			
		} else {
			
			throw new RuntimeException("Unknown DbTarget: " + dbtarg);	
		}
	}
	
	public static String getHostName(DbTarget dbtarg)
	{
		return getIpDbName(dbtarg)[0];	
	}
	
	public DatabaseBridge(DbTarget dbtarg)
	{
		_dbTarg = dbtarg;
	}
	
	public Connection createConnection() throws SQLException
	{
		return getDbConnection(_dbTarg);
	}
	
	public static Connection getDbConnection(DbTarget dbtarg) throws SQLException
	{ 
		String[] ipdbname = getIpDbName(dbtarg);
		return getDbConnection(ipdbname[0], ipdbname[1]);
	}	
	
	public static Connection getAdnConnection(DbTarget dbtarg) throws SQLException
	{ 
		String[] ipdbname = getIpDbName(dbtarg);
		return getDbConnection(ipdbname[0], "adnetik");
	}		
	
	public static Connection getDbConnection(String dbaddress, String dbname) throws SQLException
	{ 
		DbUtil.doMysqlClassInit();
				
		String jdbcurl = Util.sprintf("jdbc:mysql://%s/%s", dbaddress, dbname);

		//String jdbcurl = Util.sprintf("jdbc:mysql://66.117.49.93/%s", dbname);
		
		DbCred usercred = DbUtil.lookupCredential();
		
		//String jdbcurl = Util.sprintf("jdbc:mysql://localhost/%s?user=root&password=GaneshSQL", dbname);
		
		Connection conn = DriverManager.getConnection(jdbcurl, usercred.getUserName(), usercred.getPassWord());

		return conn;		
	}	
	
	public static void populateDefMap(Map<DimCode, Integer> defmap, String defname, DbTarget dbtarg)
	{
		for(DimCode dcode : DimCode.values())
		{
			if(!dcode.hasCatalog())
				{ continue; }
			
			// The gender+city defmaps are not set up correctly, they need to be updated for them to work
			// currcode has no defaults, the code MUST be present.
			// This set should probably be in its own method.
			if(dcode == DimCode.city || dcode == DimCode.currcode)
				{ continue; }
			
			// Util.pf("Trying to do lookup for dcode=%s, defname=%s\n", dcode, defname);
			
			String lookupsql = Util.sprintf("SELECT id FROM cat_%s WHERE LOWER(name) = '%s'", dcode, defname.toLowerCase());
			List<Long> reslist = DbUtil.execSqlQuery(lookupsql, new DatabaseBridge(dbtarg));
			
			// Should really be strict equals 1, but some catalogs are screwed up
			Util.massert(reslist.size() >= 1, "Found %d results for default map query on dimcode %s, pref %s, expected 1",
				reslist.size(), dcode, defname);
			defmap.put(dcode, Integer.valueOf(""+reslist.get(0)));
			
			//Util.pf("Found good code %d for pref=%s for cat=%s\n", reslist.get(0), defname, dcode);				
		}
	}
	
	// Returns the set of IntFacts used by the given aggregation on the given DBtarget.
	public static SortedSet<IntFact> getIntFactSet( AggType atype, DbTarget dbtarg)
	{
		SortedSet<IntFact> factset = Util.treeset();
		
		for(String oneid : getTableColNames(atype, dbtarg))
		{
			if(oneid.startsWith("NUM_"))
			{
				IntFact ifact = IntFact.valueOf(oneid.substring(4).toLowerCase());	
				factset.add(ifact);
			}			
		}
		
		return factset;		
	}	
	
	public static SortedSet<DblFact> getDblFactSet(AggType atype, DbTarget dbtarg)
	{
		SortedSet<DblFact> factset = Util.treeset();
		
		for(String oneid : getTableColNames(atype, dbtarg))
		{
			if(oneid.startsWith("IMP_"))
			{
				DblFact dfact = DblFact.valueOf(oneid.substring(4).toLowerCase());	
				factset.add(dfact);
			}			
		}
		
		return factset;		
	}

	public static SortedSet<DimCode> getDimSet(AggType atype, DbTarget dbtarg)
	{
		SortedSet<DimCode> dimset = Util.treeset();
		
		for(String oneid : getTableColNames(atype, dbtarg))
		{
			if(oneid.startsWith("ID_"))
			{
				DimCode dimc = DimCode.valueOf(oneid.substring(3).toLowerCase());	
				dimset.add(dimc);
			}	
			
			if(oneid.startsWith("NAME_"))
			{
				DimCode dimc = DimCode.valueOf(oneid.substring(5).toLowerCase());	
				dimset.add(dimc);
			}						
		}
		
		return dimset;
	}
	
	public String toString()
	{
		return Util.sprintf("Mysql::%s", getIpDbName(_dbTarg)[0]);
	}
	
	public static Map<AggType, SortedSet<DimCode>> getDimSetMap(DbTarget dbtarg)
	{
		Map<AggType, SortedSet<DimCode>> dmap = Util.treemap();
		for(AggType atype : AggType.values())
		{
			dmap.put(atype, getDimSet(atype, dbtarg));
		}
		return dmap;
	}
	
	private static List<String> getAdGeneralColNames(DbTarget dbtarg)
	{
		return getTableColNames(AggType.ad_general, dbtarg);
	}
	
	// This is a safety check: want to make sure all the relevant columns in the RENAME VIEW are also in the STAGING 
	// table.
	static void checkStage2MainNameOverlap( DbTarget dbtarg)
	{
		for(AggType atype : AggType.values())
		{
			Map<String, String> viewmap = getTableNameTypeMap(getRenameViewName(atype), dbtarg);
			Map<String, String> stgemap = getTableNameTypeMap(getStageTableName(dbtarg, atype), dbtarg);
			
			int checkcol = 0;
			
			for(String viewcol : viewmap.keySet())
			{
				if(viewcol.startsWith("ID_") || viewcol.startsWith("NUM_") || viewcol.startsWith("IMP_"))
				{
					Util.massert(stgemap.containsKey(viewcol), 
						"View column %s not found in staging table", viewcol);
					
					if(!stgemap.get(viewcol).equals(viewmap.get(viewcol)))
					{
						Util.pf("Warning, column %s::%s is type %s in rename view but %s in stage table\n",
								getRenameViewName(atype), viewcol,
								viewmap.get(viewcol), stgemap.get(viewcol));
						
						
					}
					
					// TODO: add this back in
					Util.massert(stgemap.get(viewcol) == viewmap.get(viewcol),
						"For column %s::%s, found type %s in stage table but %s in view", 
						getRenameViewName(atype), viewcol,
						viewmap.get(viewcol), stgemap.get(viewcol));

					checkcol++;
				}
			}
			
			Util.pf("Column name overlap for %s vs %s checked out okay, %d columns checked\n",
				getRenameViewName(atype), getStageTableName(dbtarg, atype), checkcol);
		}
	}
	
	static Map<String, String> getTableNameTypeMap(String tablename, DbTarget dbtarg)
	{
		Map<String, String> typemap = Util.treemap();
		
		try { 
			
			Connection conn = (new DatabaseBridge(dbtarg)).createConnection();
			
			PreparedStatement pstmt = conn.prepareStatement(Util.sprintf("SELECT * FROM %s LIMIT 1", tablename));
			ResultSet rs = pstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			
			for(int i = 1; i <= rsmd.getColumnCount(); i++)
			{
				String colname = rsmd.getColumnName(i);
				String typestr = rsmd.getColumnTypeName(i);
				typemap.put(colname, typestr);
			}
			conn.close();
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);	
		} 
		
		return typemap;
	}
	
	private static String getCreateRenameViewSql(DbTarget dbtarg, AggType atype)
	{
		Util.massert(dbtarg == DbTarget.internal);
		
		List<String> sel_list = Util.vector();
		
		sel_list.add(" SELECT * ");
		
		for(DimCode dcode : DimCode.values())
		{
			if(dcode.hasColumnRename())
			{
				String sclause = Util.sprintf(" %s AS ID_%s ",
					dcode.getBaseTableColName(), dcode.toString().toUpperCase());
				
				sel_list.add(sclause);
			}
		}
		
		return Util.sprintf(" CREATE OR REPLACE VIEW %s AS %s FROM %s",
			getRenameViewName(atype), Util.join(sel_list, ","), getAggTableName(DbTarget.internal, atype));
			
	}
	
	
	public static List<String> getTableColNames(AggType atype, DbTarget dbtarg)
	{
		// String sql = "DESCRIBE " + getAggTableName(dbtarg, atype);	
		String sql = "DESCRIBE " + getRenameViewName(atype);	
		List<String> colnames = Util.vector();
		
		try {
			Connection conn = getDbConnection(dbtarg);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				String oneid = rset.getString(1);
				colnames.add(oneid);
			}
			
			conn.close();
			
			return colnames;
			
		} catch (Exception ex) {
			
			ex.printStackTrace();
			throw new RuntimeException(ex);
		} 				
	}
	
	
	public static int deleteOld(AggType atype, String daycode, DbTarget dbtarg)
	{
		// TODO: this should always be ENTRY_DATE in the future
		int delrows = 0;
		String datefield = (atype == AggType.ad_general ? "id_date" : "entry_date");
		//int DELETE_BATCH = 10000;
		
		try {
			Connection conn = getDbConnection(dbtarg);
			
			String delsql = Util.sprintf("DELETE FROM %s WHERE %s = date('%s')", atype.toString(), datefield, daycode);
			
			//String delsql = Util.sprintf("DELETE FROM %s WHERE %s = date('%s') LIMIT %d", atype.toString(), datefield, daycode, DELETE_BATCH);
			delrows = execSqlUpdate(delsql, conn);
			
			conn.close();
			return delrows;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}
	
	public static int deleteTest(AggType atype, String daycode, DbTarget dbtarg)
	{
		// TODO: this should always be ENTRY_DATE in the future
		int delrows = 0;
		String datefield = (atype == AggType.ad_general ? "id_date" : "entry_date");
		
		try {
			Connection conn = getDbConnection(dbtarg);
			
			String sql = Util.sprintf("SELECT * FROM %s WHERE %s = date('%s')", atype.toString(), datefield, daycode);
			Util.pf("\nSQL is %s", sql);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				rset.deleteRow();
				delrows++;
				
				if((delrows % 100) == 0)
				{
					Util.pf("\nDeleted %d rows", delrows);	
				}
			}
			
			conn.close();
			return delrows;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}	
	
	
	// TODO: change fctlist into a set of enumerations also
	public static int loadFromFile(File infile, SortedSet<DimCode> dimset, SortedSet<IntFact> intset, SortedSet<DblFact> dblset, DbTarget dbtarg)
	{
		List<String> collist = Util.vector();
		
		if(RANDOM_CHECK.nextDouble() < .1)
		{
			Util.pf("Random error check on column names ... ");			
			Util.massert(dimset.equals(getDimSet(AggType.ad_general, dbtarg)), "Dimension sets not equal");
			Util.massert(intset.equals(getIntFactSet(AggType.ad_general, dbtarg)), "Int Fact sets not equal");
			Util.massert(dblset.equals(getDblFactSet(AggType.ad_general, dbtarg)), "Int Fact sets not equal");
			Util.pf(" ... passed.\n");
		}
		
		for(DimCode col : dimset)
			{ collist.add("ID_" + col.toString().toUpperCase()); }
		
		for(IntFact col : intset)
			{ collist.add("NUM_" + col.toString().toUpperCase()); }
		
		for(DblFact col : dblset)
			{ collist.add("IMP_" + col.toString().toUpperCase()); }
		
		collist.add("ENTRY_DATE");
		
		return DbUtil.loadFromFile(infile, "ad_general", collist, new DatabaseBridge(dbtarg));
	}
	
	private static String partCodeFromStr(String daycode)
	{
		Integer remdash = TimeUtil.dayCode2Int(daycode);
		return Util.sprintf("part%d", remdash);
	}	
	
	private static String partCodeFromInt(int remdash)
	{
		return Util.sprintf("part%d", remdash);
	}
	
	private static String subPartCode(int daycode, int subcode)
	{
		// part20120130_sub15
		return Util.sprintf("part%d_sub%d", daycode, subcode);	
	}
	
	public static Set<String> getPartitionNameSet(DbTarget dbtarg)
	{
		String sql = "select partition_name from information_schema.partitions where table_name = 'ad_general'";
		Set<String> pset = Util.treeset();
		
		try {
			
			Connection conn = getDbConnection(dbtarg);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				pset.add(rset.getString(1));
				//Util.pf("\nPart name is %s", rset.getString(1));	
			}
				
			conn.close();	
			return pset;
			
		} catch (Exception ex) {
			
			ex.printStackTrace();
			throw new RuntimeException(ex);
		} 			
		
	}	
	
	static String getDateWhereClause(String datefield)
	{
		Calendar mycal = TimeUtil.getToday();
		for(int i = 0; i < 60; i++)
			{ mycal = TimeUtil.dayBefore(mycal); }
		
		
		String wc = Util.sprintf(" %s > date('%s') ", datefield, TimeUtil.cal2DayCode(mycal));
		return wc;
	}
	
	static int execSqlUpdate(String sql, DbTarget dbtarg)
	{
		return DbUtil.execSqlUpdate(sql, new DatabaseBridge(dbtarg));
	}	
	
	static int execSqlUpdate(String sql, Connection conn) throws SQLException
	{
		return DbUtil.execSqlUpdate(sql, conn);
	}	
	
	public static Map<CurrCode, Double> readExchangeRateInfo(String daycode, DbTarget dbtarg)
	{
		Map<CurrCode, Double> excmap = Util.treemap();
		String sql = "SELECT CURR_CODE, ONE_USD_BUYS FROM daily_exchange_rates WHERE ID_DATE = ?";
		
		try {
			
			Connection conn = getDbConnection(dbtarg);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, daycode);
			
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				CurrCode ccode = CurrCode.valueOf(rset.getString(1));
				double rate = rset.getDouble(2);
				excmap.put(ccode, rate);
			}
			conn.close();
			
		} catch (Exception ex) {
			
			ex.printStackTrace();
			throw new RuntimeException(ex);
		} 
		
		return excmap;
	}
	
	public static int updateExchangeRateInfo(String daycode, Map<String, Double> dollmap, ConnectionSource csource)
	{
		int rows = 0;		
		
		for(CurrCode ccode : CurrCode.values())
		{
			rows += updateExchangeRateInfo(daycode, ccode, dollmap.get(ccode.toString()), csource);			
			
			// Enter the same information for TOMORROW, which we use as an estimate of the exchange rates until we get better information.
			// Tomorrow we'll overwrite the approximate value with a new value.
			rows += updateExchangeRateInfo(TimeUtil.dayAfter(daycode), ccode, dollmap.get(ccode.toString()), csource);			
		}
		
		return rows;
	}
	
	public static int updateExchangeRateInfo(String daycode, CurrCode ccode, double usdbuys, ConnectionSource connsrc)
	{
		// Do this using delete/insert, instead of smart REPLACE INTO, to make sure 
		// it works for both NTZ and MYSQL
		{
			String delsql = Util.sprintf("DELETE FROM daily_exchange_rates WHERE id_date = '%s' AND curr_code = '%s'",
								daycode, ccode);
			
			DbUtil.execSqlUpdate(delsql, connsrc);
			
		} {
			String insql = Util.sprintf("INSERT INTO daily_exchange_rates (id_date, curr_code, one_usd_buys) VALUES ('%s', '%s', %.04f)",
								daycode, ccode, usdbuys);
			
			int inrows = DbUtil.execSqlUpdate(insql, connsrc);
			return inrows;
		}
	}	
	
	
	public static int updateLatestExchangeRates(DbTarget targ)
	{
		String sql = "UPDATE latest_exchange_rates ler, daily_exchange_rates der SET ler.ID_DATE = der.ID_DATE, ";
		sql += " ler.ONE_USD_BUYS = der.ONE_USD_BUYS  WHERE ler.CURR_CODE = der.CURR_CODE ";
		sql += " AND (der.CURR_CODE, der.ID_DATE ) IN (SELECT der2.CURR_CODE, MAX(der2.ID_DATE) FROM daily_exchange_rates der2 GROUP BY der2.CURR_CODE )" ;

		// Util.pf("SQL is \n%s\n", sql);
		
		return DbUtil.execSqlUpdate(sql, new DatabaseBridge(targ));
	}
	
	public static Map<String, Integer> readCatalog(DimCode cat, DbTarget dbtarg)
	{
		return readCatIdMap(cat, getCatalogSelectSql(cat), dbtarg);
	}
	
	private static String getCatalogSelectSql(DimCode cat)
	{
		String centersql;
		
		if(cat == DimCode.region)
			{ centersql = Util.sprintf(" concat(id_country, '%s', code) ", Util.DUMB_SEP); }
		else
			{ centersql = (_NAMEONLY.contains(cat) ? "name" : "code"); }
		
		return centersql;
	}
	
	static Map<String, Integer> readCatIdMap(DimCode cat, String centersql, DbTarget dbtarg)
	{
		Map<String, Integer> catmap = Util.treemap();
		String sql = Util.sprintf("SELECT id, %s FROM cat_%s", centersql, cat);
		
		try {
			
			Connection conn = getDbConnection(dbtarg);
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				// All lookups are done in lowercase
				String colname = rset.getString(2);
				int colid = rset.getInt(1);
				
				// TODO: this is not the end of the world, since some fields have
				// Unknown_RMX 
				if(colname == null)
				{
					Util.pf("\nWarning: null value found for DimCode %s", cat);	
					continue;
				}
				
				String lcase = colname.toLowerCase();
				
				// TODO this is a temp workaround for the problem of having bad currcode=0 data rows in the DB,
				// take it out as soon as this is fixed
				if(cat == DimCode.currcode && lcase.equals("usd") && colid == 0)
					{ continue; }
				
				Util.massert(lcase.equals("ot") || !catmap.containsKey(lcase) || cat == DimCode.city , "Duplicate colname %s found for dimension %s", lcase, cat);
				
				
				catmap.put(lcase, colid);
				//Util.pf("\nid= %d, code=%s", rset.getInt(1), rset.getString(2));	
			}
			conn.close();
			
		} catch (Exception ex) {
			
			ex.printStackTrace();
			throw new RuntimeException(ex);
		} 
		
		return catmap;
	}	

	public static String getAggTableName(DbTarget targ, AggType atype)
	{
		// ad_general --> fast_general
		// ad_domain --> fast_domain
		String s = atype.toString();
		String pref = (targ == DbTarget.internal ? "fast_" : "ad_");
		return pref + s.substring(3);
	}	
	
	public static String getRenameViewName(AggType atype)
	{
		String s = atype.toString();
		String pref = "__rename_view_";
		return pref + s.substring(3);		
	}
	
	public static String getStageTableName(DbTarget targ, AggType atype)
	{
		return getAggTableName(targ, atype) + "_stage";
	}	
	
	
	static List<String> getTableColNames(String tablename, DbTarget dbtarg)
	{
		String descsql = Util.sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' AND table_schema = 'bm_etl'", tablename);
		return DbUtil.execSqlQuery(descsql, new DatabaseBridge(dbtarg));
	}
	
	public static void main(String[] args) throws Exception
	{
		Map<String, String> optargs = Util.getClArgMap(args);

		String op = args[0];
		
		if(op.equals("runquery")) {
			
			DbTarget dbtarg = DbTarget.valueOf(args[1]);
			
			String sqlfile = args[2];
			String csvfile = args[3];
			
			PrintStream pout = new PrintStream(new FileOutputStream(csvfile));
			
			// Util.pf("Going to run a query on sql file %s\n", sqlfile);
			
			Connection conn = getDbConnection(dbtarg);
			
			String sql = Util.join(FileUtils.readFileLinesE(sqlfile), "\n");
			
			DbUtil.sqlQuery2Csv(conn, sql, pout, true);
			
			conn.close();
			pout.close();
			
		} else if(op.equals("checkoverlap")) {
		
			/*
			Map<String, String> typemap = getTableNameTypeMap("fast_general_stage", DbTarget.internal);
			
			for(String colname : typemap.keySet())
			{
				Util.pf("Colname %s maps to type %s\n", colname, typemap.get(colname));	
				
			}
			*/
			
			checkStage2MainNameOverlap(DbTarget.internal);
			
		} else if(op.equals("showrename")) {
			
			Util.pf("%s\n", getCreateRenameViewSql(DbTarget.internal, AggType.ad_general));
			Util.pf("%s\n", getCreateRenameViewSql(DbTarget.internal, AggType.ad_domain));
			
		} else if(op.equals("popdefmap")) {
			
			Map<DimCode, Integer> defmap = Util.treemap();
			populateDefMap(defmap, "Unknown", DbTarget.external);
			
		} else {
			
			throw new RuntimeException("Unknown op code: " + op);	
		}
		
	}
}
