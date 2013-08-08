package com.adnetik.shared;

import java.util.*;
import java.sql.*;
import java.io.*;

import com.adnetik.shared.*;

/**
 * DatabaseBridge
 */
public class DbUtil {

	private static boolean _MYSQL_CLASS_INIT = false;
	private static boolean _NETEZZA_CLASS_INIT = false;
	
	private static DbCred _USER_CREDENTIAL;
	
	public enum DbMachine { 
		internal("thorin-internal.adnetik.com"),
		external("thorin.adnetik.com");
		
		private String _hostInfo;
		
		DbMachine(String hinfo)
		{
			_hostInfo = hinfo;
		}
		
		
		public String getHostInfo()
		{
			return _hostInfo;
		}
	}
	
	// Use package-specific enums that implement this interface.
	public interface DbTable 
	{
		public DbMachine getMachine();
		public String getDbName();
	}
		
	public interface DbCred
	{
		public String getUserName();
		public String getPassWord();
	}
	
	// TODO: this needs to be changed
	public static Connection getDbConnection(String hostinfo, String dbname) throws SQLException
	{ 		
		DbCred dbc = lookupCredential();
		return getDbConnection(hostinfo, dbname, dbc.getUserName(), dbc.getPassWord());
	}

	public static Connection getDbConnection(String hostinfo, String dbname, String uname, String pword) throws SQLException
	{ 
		doMysqlClassInit();
		
		String jdbcurl = Util.sprintf("jdbc:mysql://%s/%s", hostinfo, dbname);
		Connection conn = DriverManager.getConnection(jdbcurl, uname, pword);
		return conn;		
	}	
	
	public static interface ConnectionSource
	{
		public abstract Connection createConnection() throws SQLException;	
	}
	
	public static class ThorinExternalConn implements ConnectionSource
	{
		private String _dbName;
		
		public static final String HOST_NAME = "thorin.adnetik.com";
		
		public ThorinExternalConn()
		{
			this("adnetik");	
		}
		
		public ThorinExternalConn(String dbn)
		{
			_dbName = dbn;
		}
		
		public Connection createConnection() throws SQLException
		{
			return getDbConnection(HOST_NAME, _dbName);
		}
	}
	
	public static class SimpleSource implements ConnectionSource
	{
		private DbTable _dbTable;
		private DbCred _dbCred;
		
		public SimpleSource(DbTable dtab)
		{
			this(dtab, lookupCredential());	
		}
		
		public SimpleSource(DbTable dtab, DbCred dbc)
		{
			_dbTable = dtab;
			_dbCred = dbc;
		}
		
		public Connection createConnection() throws SQLException
		{
			return getDbConnection(
				_dbTable.getMachine().getHostInfo(), _dbTable.getDbName(), 
				_dbCred.getUserName(), _dbCred.getPassWord());
		}
		
	}
	
	public static DbCred lookupCredential()
	{
		if(_USER_CREDENTIAL == null)
		{
			// This method SUCKS because we have to have the MySQL credential on 
			/*
				final String username = System.getProperty("user.name");
				
				final List<String> passlist = FileUtils.readFileLinesE(Util.sprintf("/home/%s/.ssh/SQL_PASS.txt", username));
				
				Util.massert(passlist.size() == 1, "Expected to find single line in password file, found %d",
					passlist.size());
			*/
			
			_USER_CREDENTIAL = new DbCred() {
				public String getUserName() { return "burfoot"; }
				public String getPassWord() { return "data_101?"; }
			};
		}
		
		return _USER_CREDENTIAL;
	}
	
	public static List<String> showTables(Connection conn) throws SQLException
	{
		List<String> tablist = execSqlQuery("SHOW TABLES", conn);
		return tablist;
	}
	
	public static int execWithTime(String sql, String opcode, ConnectionSource csource)
	{
		return execWithTime(sql, opcode, csource, new SimpleMail("GIMP MAIL"));	
	}
	
	public static int execWithTime(String sql, String opcode, Connection conn) throws SQLException
	{
		return execWithTime(sql, opcode, conn, new SimpleMail("GIMP MAIL"));	
	}	
	
	public static int execWithTime(String sql, String opcode, ConnectionSource csource, SimpleMail logmail)
	{
		double startup = Util.curtime();
		logmail.pf("Running operation: %s ", opcode);
		int rows = execSqlUpdate(sql, csource);
		double timesecs = (Util.curtime() - startup)/1000;
		double rowspersec = ((double) rows)/timesecs;
		logmail.pf(" ... done, %d rows affected, took %.03f secs, %.03f rows/sec\n", rows, timesecs, rowspersec);
		return rows;		
	}
	
	public static int execWithTime(String sql, String opcode, Connection conn, SimpleMail logmail) throws SQLException
	{
		double startup = Util.curtime();
		logmail.pf("Running operation: %s ", opcode);
		int rows = execSqlUpdate(sql, conn);
		double timesecs = (Util.curtime() - startup)/1000;		
		double rowspersec = ((double) rows)/timesecs;
		logmail.pf(" ... done, %d rows affected, took %.03f secs, %.03f rows/sec\n", rows, timesecs, rowspersec);
		return rows;		
	}	
	
	
	public static int execSqlUpdate(String sql, ConnectionSource csource)
	{
		try { 
			Connection conn = csource.createConnection();	
			int rows = execSqlUpdate(sql, conn);
			conn.close();
			return rows;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}		
	}
	
	public static int execSqlUpdate(String sql, Connection conn) throws SQLException
	{
		PreparedStatement pstmt = conn.prepareStatement(sql);
		int rows = pstmt.executeUpdate();
		return rows;
	}		
	
	public static <A> List<A> execSqlQuery(String sql, ConnectionSource csource)
	{
		try { 
			Connection conn = csource.createConnection();	
			List<A> rlist = execSqlQuery(sql, conn);
			conn.close();
			return rlist;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <A> List<A> execSqlQuery(String sql, Connection conn) throws SQLException
	{
		List<A> reslist = Util.vector();
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		
		while(rset.next())
		{
			A a = (A) rset.getObject(1);
			reslist.add(a);
		}
		
		return reslist;
	}		

	
	public static <A,B> List<Pair<A,B>> execSqlQueryPair(String sql, ConnectionSource csource)
	{
		try { 
			Connection conn = csource.createConnection();	
			List<Pair<A, B>> rlist = execSqlQueryPair(sql, conn);
			conn.close();
			return rlist;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}	
	
	@SuppressWarnings("unchecked")	
	public static <A,B> List<Pair<A, B>> execSqlQueryPair(String sql, Connection conn) throws SQLException
	{
		List<Pair<A, B>> reslist = Util.vector();
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		
		while(rset.next())
		{
			A a = (A) rset.getObject(1);
			B b = (B) rset.getObject(2);
			reslist.add(Pair.build(a, b));
		}
		
		return reslist;
	}			
	
	
	public static <A,B> void popMapFromQuery(Map<A, B> topop, String sql, ConnectionSource csource)
	{
		try { 
			Connection conn = csource.createConnection();	
			popMapFromQuery(topop, sql, conn);
			conn.close();

		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}	
	
	@SuppressWarnings("unchecked")
	public static <A,B> void popMapFromQuery(Map<A, B> topop, String sql, Connection conn) throws SQLException
	{
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		
		while(rset.next())
		{
			A a = (A) rset.getObject(1);
			B b = (B) rset.getObject(2);
			Util.massert(!topop.containsKey(a), "Error: duplicate key found: %s", a);
			topop.put(a, b);
		}
		
		rset.close();
	}			
		
	
	
	
	public static void doMysqlClassInit()
	{
		if(_MYSQL_CLASS_INIT)
			{ return ; }
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			_MYSQL_CLASS_INIT = true;
		} catch (Exception ex )  {
			
			throw new RuntimeException(ex);	
		}
	}
	
	public static int executeUpdate(Connection conn, String sql) throws SQLException
	{
		PreparedStatement pstmt = conn.prepareStatement(sql);
		int rows = pstmt.executeUpdate();
		return rows;
	}
	
	public static int executeUpdateE(Connection conn, String sql)
	{
		try { return executeUpdate(conn, sql); }	
		catch (SQLException sqlex) { throw new RuntimeException(sqlex); }
	}	
	
	public static double sqlQuery2Map(Connection conn, String sql, Map<String, Vector<String>> resmap) throws SQLException
	{
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		return resultSet2Map(rset, resmap);
	}
	
	public static double resultSet2Map(ResultSet rset, Map<String, Vector<String>> rmap)  throws SQLException
	{
		double startup = Util.curtime();
		rmap.clear();
		ResultSetMetaData rsmd = rset.getMetaData();
		
		for(int i = 1; i <= rsmd.getColumnCount(); i++)
		{
			String cname = rsmd.getColumnLabel(i);
			rmap.put(cname, new Vector<String>());
		}	
			
		while(rset.next())
		{
			// ahh
			for(String colname : rmap.keySet())
				{ rmap.get(colname).add(rset.getString(colname)); }
		}
	
		return (Util.curtime()-startup)/1000;
	}
	
	// Shouldn't this just read in all the data of the result set, and then
	// Put it in some intermediate format, that has a generic toCsv(), toTsv(), toXml() methods?
	public static int resultSet2Csv(ResultSet rset, PrintStream output, boolean incheader) throws IOException, SQLException
	{
		int numrec = 0;
		
		ResultSetMetaData rsmd = rset.getMetaData();
		int numcol = rsmd.getColumnCount();
		
		if(incheader)
		{
			for(int i = 1; i <= numcol; i++)
			{
				output.print(rsmd.getColumnLabel(i));
				
				if(i < numcol)
					{ output.print(","); }
			}	
			
			output.print("\n");
		}
		
		
		while(rset.next())
		{
			numrec++;
			
			for(int i = 1; i <= numcol; i++)
			{
				output.print(rset.getString(i));
				
				if(i < numcol)
					{ output.print(","); }
			}
			
			if(!rset.isLast())
				{ output.print("\n"); }
		}
		
		return numrec;
	}
	
	public static int sqlQuery2Csv(Connection conn, String sql, PrintStream output, boolean incheader)
	throws IOException, SQLException
	{
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		return resultSet2Csv(rset, output, incheader);
	}
	
	public static class QueryCollector
	{
		private LinkedHashMap<String, Vector<String>> _resultData = new LinkedHashMap<String, Vector<String>>();
		
		private boolean _incHeader = false;
		
		private String delim = "\t";
		
		public QueryCollector(String sql, ConnectionSource csource) 
		{
			grabData(sql, csource);
		}
		
		public QueryCollector(String sql, Connection conn) throws SQLException
		{
			grabData(sql, conn);
		}
		
		public QueryCollector(ResultSet rset) throws SQLException
		{
			grabData(rset);
		}
		
		void grabData(String sql, ConnectionSource csource)
		{
			try  { grabData(sql, csource.createConnection()); }
			catch (SQLException sqlex) {
				throw new RuntimeException(sqlex);	
			}
		}		
		
		
		void grabData(String sql, Connection conn) throws SQLException
		{
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			grabData(rset);			
		}
		
		void grabData(ResultSet rset) throws SQLException
		{
			Util.massert(_resultData.isEmpty(),
				"You cannot reuse these objects, already have result data with %d columns", _resultData.size());
			
			ResultSetMetaData rsmd = rset.getMetaData();
						
			for(int i = 1; i <= rsmd.getColumnCount(); i++)
			{
				String c_label = rsmd.getColumnLabel(i);
				_resultData.put(c_label, new Vector<String>());
			}				
			
			while(rset.next())
			{
				for(String colname : _resultData.keySet())
					{ _resultData.get(colname).add(rset.getString(colname)); }
			}			
		}
		
		public void  setIncHeader(boolean ih)
		{
			_incHeader = ih;	
		}
		
		public List<String> getRow(int i)
		{
			List<String> rowlist = Util.vector();
			
			for(Vector<String> coldata : _resultData.values())
				{ rowlist.add(coldata.get(i)); }
			
			return rowlist;
		}
		
		public int getNumRec()
		{
			for(Vector<String> onevec : _resultData.values())
				{ return onevec.size(); }
			
			throw new RuntimeException("Result DAta is empty, must call grabData(..) first");
		}
				
		public void writeResult(BufferedWriter bwrite) throws IOException
		{
			if(_incHeader)
			{
				FileUtils.writeRow(bwrite, delim, _resultData.keySet());
			}		
			
			int nr = getNumRec();
			
			for(int i = 0; i < nr; i++)
			{
				FileUtils.writeRow(bwrite, delim, getRow(i));
			}
		}
		
		public void writeResultE(BufferedWriter bwrite)
		{
			try { writeResult(bwrite); }
			catch (IOException ioex) { throw new RuntimeException(ioex); }	
		}
		
		public void writeResult(File onefile) throws IOException
		{
			BufferedWriter bwrite = FileUtils.getWriter(onefile.getAbsolutePath());
			writeResult(bwrite);
			bwrite.close();
		}

		public void writeResultE(File targfile)
		{
			try { writeResult(targfile); }
			catch (IOException ioex) { throw new RuntimeException(ioex); }	
		}		
	}
	
	public static int loadFromFile(File infile, String tabname, List<String> collist, ConnectionSource csource)
	{
		try { 
			Connection conn = csource.createConnection();	
			int uprows = loadFromFile(infile, tabname, collist, conn);
			conn.close();
			return uprows;
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}
	}
	
	public static String loadFromFileSql(File infile, String tabname, List<String> collist)
	{ return loadFromFileSql(infile, tabname, collist, true); }
	
	public static String loadFromFileSql(File infile, String tabname, List<String> collist, boolean islocal)
	{
		String colnames = Util.join(collist, ",");
		
		return Util.sprintf("LOAD DATA %s INFILE '%s' INTO TABLE %s ( %s ) ", 
			(islocal ? "LOCAL" : ""), infile.getAbsolutePath(), tabname, colnames);	
	}
	
	public static int loadFromFile(File infile, String tabname, List<String> collist, Connection conn) throws SQLException
	{
		String sql = loadFromFileSql(infile, tabname, collist);
		
		// Util.pf("\nSQL statement is %s", sql);
		
		PreparedStatement pstmt = conn.prepareStatement(sql);
		int rows = pstmt.executeUpdate();
		return rows;		
	}
	
	public static Set<String> getColNameSet(String tabname, ConnectionSource csource)
	{
		String sql = "DESCRIBE " + tabname;
		List<String> collist = execSqlQuery(sql, csource);
		return new TreeSet<String>(collist);
		
	}	
	
	public static Set<String> getColOverlapSet(String table_a, String table_b, ConnectionSource csource)
	{
		Set<String> overlap = getColNameSet(table_a, csource);
		overlap.retainAll(getColNameSet(table_b, csource));
		return overlap;
	}	
	
	
	public static class NZConnSource implements ConnectionSource
	{
		public static String NZ_HOST_ADDR = "66.117.49.50";
		
		private String _dbName; 
		private DbCred _dbCred;
		
		private synchronized static void initClass()
		{
			if(!_NETEZZA_CLASS_INIT)
			{
				try {  Class.forName("org.netezza.Driver"); }
				catch (ClassNotFoundException cnfex) {
					throw new RuntimeException("Error in initializing Netezza JDBC, probably a classpath problem");	
				}
				_NETEZZA_CLASS_INIT = true;
			}
		}
		
		public NZConnSource(String dbn, DbCred dbc)
		{
			_dbName = dbn;
			_dbCred = dbc;
		}
		
		// Deprecated, use getNetezzaConn instead
		@Deprecated
		public static NZConnSource getFastEtlConn(String targdb)
		{
			return new NZConnSource(targdb, lookupCredential());
		}			
		
		
		public static NZConnSource getNetezzaConn(String targdb)
		{
			return new NZConnSource(targdb, lookupCredential());
		}		
		
		public Connection createConnection() throws SQLException
		{
			initClass();
			
			return DriverManager.getConnection(getJdbcUrl(), _dbCred.getUserName(), _dbCred.getPassWord());
		}
		
		
		private String getJdbcUrl()
		{
			return Util.sprintf("jdbc:netezza://%s/%s", NZ_HOST_ADDR, _dbName);
		}
		
		public String toString()
		{	
			return Util.sprintf("Netezza::%s", _dbName);
		}
		
	}
	
	public static class InfSpooler
	{
		ConnectionSource _connSource;
		
		Map<String, String> _dataMap = Util.treemap();		
		private Set<String> _mapKeySet;
		
		Random _randChecker = new Random();
		
		Set<Enum> _fieldSet = Util.treeset();
		
		private String _tableName;
		private String _infPath;
		private BufferedWriter _infWriter;
		
		private int _batchSize = 10000; // default
		private int _batchCount = 0;
		
		int _errCount = 0;
		
		private SimpleMail _logMail = new SimpleMail("GIMP");
		
		public InfSpooler(Enum[] fields, String infpath, ConnectionSource csource, String tname)
		{
			_fieldSet.addAll(Arrays.asList(fields));
			_infPath = infpath;
			_connSource = csource;
			_tableName = tname;
		}
		
		public void setBatchSize(int bs)
		{
			Util.massert(_infWriter == null && _batchCount == 0,
				"Must set batch size before sending any data");
			
			_batchSize = bs;
		}
		
		public void setLogMail(SimpleMail sm)
		{
			_logMail = sm;
		}
		
		public void setStr(Enum fname, String val)
		{
			setGen(fname, "Str", val);
		}
		
		public void setInt(Enum fname, Integer val)
		{
			setGen(fname, "Int", val);
		}
		
		public void setLng(Enum fname, Long val)
		{
			setGen(fname, "Lng", val);
		}		
		
		public void setDate(Enum fname, String daycode)
		{
			setGen(fname, "Date", daycode);
		}
		
		public void setDbl(Enum fname, Double val)
		{
			setGen(fname, "Dbl", val);
		}		
		
		public void setObj(Enum fname, Object obj)
		{
			setGen(fname, "", obj);
		}
		
		private void setGen(Enum fname, String suff, Object obj)
		{
			randomCheck(fname, suff);
			String realfield = fname.toString().substring(0, fname.toString().length()-suff.length());
			Util.putNoDup(_dataMap, realfield, obj.toString());
		}
		
		private void randomCheck(Enum fname, String suff)
		{
			_errCount--;
			
			if(_errCount <= 0)
			{
				Util.massert(fname.toString().endsWith(suff),
					"Bad enum name %s, need suffix %s", fname, suff);
				
				Util.massert(_fieldSet.contains(fname), 
					"Field %s not found in field set", fname);				
				
				_errCount = _randChecker.nextInt(10000)+1;
			}
		}
		
		public void flushRow() throws IOException
		{
			Util.massert(_dataMap.size() == _fieldSet.size(),
				"Have %d fields set out of %d total", 
				_dataMap.size(), _fieldSet.size());
			
			// Reinitialize writer if necessary
			if(_infWriter == null)
				{ _infWriter = FileUtils.getWriter(_infPath);	 }
			
			_batchCount++;
			
			for(String value : _dataMap.values())
			{
				_infWriter.write(value);
				_infWriter.write("\t");
			}

			_infWriter.write("\n");
			
			if(_mapKeySet == null)
			{ 
				_mapKeySet = Util.treeset();
				_mapKeySet.addAll(_dataMap.keySet());
			}
			
			// Clear datamap
			_dataMap.clear();			
			
			if(_batchCount == _batchSize)
				{ upload2db(); }
		}
		
		private List<String> getColList()
		{
			Util.massert(_mapKeySet != null && !_mapKeySet.isEmpty(), "Map key set not initialized");
			return new Vector<String>(_mapKeySet);
		}
		
		public void upload2db() throws IOException
		{
			if(_infWriter != null)
				{ _infWriter.close();}
			
			_infWriter = null;			
			
			List<String> collist = getColList();
			Util.massert(!collist.isEmpty(), "Column list is empty");
			
			int uprows = loadFromFile(new File(_infPath), _tableName, collist, _connSource);
			_logMail.pf("InfSpooler: Uploaded %d rows of data, %d columns, batchcount is %d\n", 
				uprows, collist.size(), _batchCount);
			
			// Reset the batch count
			_batchCount = 0;
		}
		
		public void finish() throws IOException
		{
			finish(false);	
		}
		
		public void finish(boolean cleaninf) throws IOException
		{
			upload2db();
			
			if(cleaninf)
			{
				File inff = new File(_infPath);
				if(inff.exists())
					{ inff.delete(); }
			}
		}
	}
	
	// This is useful in PreparedStatement 
	public  static String getNQMarkStr(int N)
	{
		String[] qmarkarr = new String[N];	
		Arrays.fill(qmarkarr, "?");
		return Util.join(qmarkarr, " , ");
	}
	
	public static void main(String[] args)
	{
		// File f = new File("~/.ssh/lookuptest.txt");
		
		String username = System.getProperty("user.name");
		
		Util.pf("User name is %s\n", username);
		
		List<String> flist = FileUtils.readFileLinesE(Util.sprintf("/home/%s/.ssh/lookuptest.txt", username));
		
		Util.pf("FList is %s\n", flist);
		
	}
	
}
