package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.DbUtil.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

public class NetezzaAdBoardUpdate
{
	// old = to delete, new = just insert, mod = delete+insert, ok = no changes
	private enum RecStatus  { oldrec, newrec, modrec, ok_rec };
	
	private DayLogMail _logMail;
	
	private SortedMap<String, DbTable> _adbTabMap = Util.treemap();
	
	// This is NULL unless explicitly set
	private SortedSet<String> _targTabSet;
	
	// TODO: flesh this out
	private enum IdColEnum 
	{
		group_report_access("USER_GROUP_ID", "ACCOUNT_ID"),
		campaign_activity_pixel("CAMPAIGN_ID", "ACTIVITY_PIXEL_ID"),
		user_report_access("USER_ID", "USER_GROUP_ID", "COST");
		
		private List<String> _idList = Util.vector();
		
		IdColEnum(String... idlist)
		{
			for(String idcol : idlist)
				{ _idList.add(idcol); }
		}
		
		List<String> getIdList()
		{
			return _idList;
		}
	}
	
	private static List<String> getIdColList(String tabname)
	{
		try { 
			IdColEnum ide = IdColEnum.valueOf(tabname.toLowerCase());
			return ide.getIdList();
		} catch (IllegalArgumentException illex) {
			
			return Arrays.asList(new String[] { "ID" });	
		}
	}
		
	private static String getRowPkIdStr(DbRecord dbrec, String tabname)
	{
		List<String> rwlist = Util.vector();
		
		Util.massert(!getIdColList(tabname).isEmpty(),
			"Found empty PK ID list for table %s", tabname);
		
		for(String idcol : getIdColList(tabname))
		{
			Util.massert(dbrec.containsKey(idcol),
				"ID column '%s' not found in record %s\n", 
				idcol, dbrec);
			
			rwlist.add(dbrec.get(idcol));
		}
		
		return Util.join(rwlist, "___");
	}	
	
	// Name of table on Netezza, campaign=>DIM_CAMPAIGN
	public static String getNtzTableName(String adbtabname)
	{
		return Util.sprintf("dim_%s", adbtabname).toUpperCase();	
	}
	
	// Name external table used to populate NTZ table, campaign=>EXT_DIM_CAMPAIGN
	public static String getNtzExtTableName(String adbtabname)
	{
		return Util.sprintf("ext_dim_%s", adbtabname).toUpperCase();	
	}			

	public static void main(String[] args) throws Exception
	{		
		ArgMap argmap = Util.getClArgMap(args);
		String tabliststr = argmap.getString("tablelist", null);
		boolean dodelta = argmap.getBoolean("dodelta", true);
		
		
		NetezzaAdBoardUpdate nabu = new NetezzaAdBoardUpdate(tabliststr);

		if(dodelta)
			{ nabu.runContinual(); }
		else
			{ nabu.runFullInitProcess(); }
	}
	
	public NetezzaAdBoardUpdate(String tabliststr)
	{
		maybeInitNewMail();
		
		if(tabliststr != null)
		{
			_targTabSet = Util.treeset();			
			
			for(String onetab : tabliststr.split(","))
				{ _targTabSet.add(onetab); }
		}
		
		initAdbTableList();
	}
	
	void maybeInitNewMail()
	{
		String daycode = TimeUtil.getTodayCode();
		
		if(_logMail == null || !daycode.equals(_logMail.getDayCode()))
		{
			// Send previous log mail, if it is available
			if(_logMail != null)
			{
				_logMail.pf("Finished updates for daycode %s", _logMail.getDayCode());
				_logMail.send2admin();	
				_logMail.send2AdminList(AdminEmail.huimin, AdminEmail.sekhark, AdminEmail.raj, AdminEmail.ajaffer);
			}
			
			_logMail = new DayLogMail(this, daycode);
			
			// Tell all the table objects about the new log mail.
			for(DbTable onetab : _adbTabMap.values())
				{ onetab.setLogMail(_logMail);	}
		}
	}
	
	
	// Delete all the old dump file data; important to do to make sure
	// we're not using old data.
	void cleanDumpFileData()
	{
		for(DbTable dbtab : _adbTabMap.values())
			{ dbtab.cleanDumpFile(); }
	}
	
	// Queries NTZ to find a list of tables that need to be updated from AdBoard. 
	// To determine this, we just look for tables that have a DIM_ prefix
	void initAdbTableList()
	{
		String showtable = "SELECT OBJNAME from _V_OBJ_RELATION_XDB where database = 'FASTETL'";
		List<String> biglist = DbUtil.execSqlQuery(showtable, NZConnSource.getNetezzaConn("FASTETL"));
		
		String dimpref = "DIM_";
		
		for(String onetab : biglist)
		{
			if(onetab.startsWith(dimpref))
			{ 
				String adbname = onetab.substring(dimpref.length()).toLowerCase();
				
				// This one sucks because they use "range" as a column name!!!
				if(adbname.equals("time_window_ranges"))
					{ continue; }
				
				// Using only a special subset of tables
				if(_targTabSet != null && !_targTabSet.contains(adbname))
					{ continue; }				
				
				DbTable dbt = new DbTable(adbname, _logMail);
				dbt.initColSet();
				_adbTabMap.put(adbname, dbt);
			}
		}
		
		_logMail.pf("Found table list %s\n", _adbTabMap.keySet());
	}
	
	void runFullInitProcess()
	{
		for(DbTable dbtab : _adbTabMap.values())
		{
			try { 
				dbtab.grabDumpFile();
				dbtab.scpDump2Netezza();
				dbtab.loadDump2Db();			
			} catch (Exception ex) {
				ex.printStackTrace();	
			}
		}		
	}
	
	void runContinual() throws Exception
	{
		while(true)
		{
			runSingleDelta();
			
			for(int i = 0; i < 60; i++)
			{
				try {  Thread.sleep(15*1000);  }
				catch (InterruptedException iex) {}
				Util.pf("z");
			}

			Util.pf("\n");
			
			maybeInitNewMail();
		}
	}
	
	void runSingleDelta() throws Exception
	{
		for(DbTable dbtab : _adbTabMap.values())
		{
			File dumpfile = new File(dbtab.getDumpFilePath());
			Util.massert(dumpfile.exists(),
				"Dump file %s does not exist, must create before continuing", dumpfile);
		}		
		
		for(DbTable dbtab : _adbTabMap.values())
		{
			dbtab.doDelta();
		}
	}
	
	static class DbTable
	{
		Map<String, DbRecord> _dbTabInfo;
		
		LinkedHashSet<String> _colSet = new LinkedHashSet<String>();
		
		String _tabName; 
		
		List<String> _adbDataList; 
		
		// Distinction between _logMail and _myLogMail
		private SimpleMail _myLogMail;
		
		public DbTable(String tname, SimpleMail lmail)
		{
			_tabName = tname;
			
			_myLogMail = lmail;
		}
		
		void setLogMail(SimpleMail smail)
		{
			_myLogMail = smail;
		}
		
		// Describe table on NTZ
		void initColSet()
		{
			ConnectionSource csource = NZConnSource.getNetezzaConn("FASTETL");
			
			String descsql = Util.sprintf("select ATTNAME from _V_RELATION_COLUMN WHERE NAME = '%s' ORDER BY ATTNUM",
							getNtzTableName(_tabName));

			List<String> clist = DbUtil.execSqlQuery(descsql, csource);
			
			_colSet.addAll(clist);
			
			Util.massert(!_colSet.isEmpty(), "Empty column set");
			
			_myLogMail.pf("For table %s, columns are %s\n", _tabName, _colSet);
		}
		
		void grabDumpFile() throws Exception
		{
			String selsql = Util.sprintf("SELECT %s FROM %s", 
				Util.join(_colSet, ","), _tabName);			
			
			SmartAdPuller sap = new SmartAdPuller(selsql, 
				AdBoardPull.getAdBoardRsaPath(),
				Util.getUserName(), AdBoardPull.getAdboardMysqlCreds());
			
			
			sap.runQuery(false);
			
			if(!sap.getErrorLines().isEmpty())
			{
				for(String oneerr : sap.getErrorLines())
				{
					_myLogMail.pf("ERR: %s\n", oneerr);	
				}
				
				_myLogMail.send2admin();
				Util.massert(false, "Problem with AdBoard pull");
			}
			
			FileUtils.writeFileLinesE(sap.getOutputLines(), getDumpFilePath());
		}
		
		// Path to put the TSV response from AdBoard
		String getDumpFilePath()
		{
			return Util.sprintf("/home/%s/netezza/adbpull/dumpfiles/%s.tsv", Util.getUserName(), _tabName);
		}
		
		// Transfer SCP file to NTZ
		void scpDump2Netezza() throws IOException
		{
			String scpcall = Util.sprintf("scp -i %s %s %s@%s:%s",
				AdBoardPull.BURFOOT_RSA_PATH, getDumpFilePath(), 
				Util.getUserName(), DbUtil.NZConnSource.NZ_HOST_ADDR, 
				getDumpFilePath());
			
			
			List<String> outlist = Util.vector();
			List<String> errlist = Util.vector();
			
			Util.pf("Syscall: %s\n", scpcall);
			Util.syscall(scpcall, outlist, errlist);
			
			if(!errlist.isEmpty())
			{
				for(String oneerr : errlist)
				{
					_myLogMail.pf("ERROR: %s\n", oneerr);	
				}
				
				_myLogMail.send2admin();
				System.exit(1);
			}
			
			
			for(String outline : outlist)
			{
				_myLogMail.pf("OUT: %s\n", outline);	
			}		
			
			Util.pf("Uploaded TSV file to NTZ box, %d err lines\n", errlist.size());
		}
		
		void loadDump2Db()
		{
			ConnectionSource csource = NZConnSource.getNetezzaConn("FASTETL");
			
			// TODO: use transient external tables instead of this create/delete strategy
			{
				String extsql = Util.sprintf("CREATE EXTERNAL TABLE %s SAMEAS %s USING (dataobject ('%s') DELIMITER '\t')",
					getNtzExtTableName(_tabName), getNtzTableName(_tabName), getDumpFilePath());
				
				DbUtil.execSqlUpdate(extsql, csource);
			}
			
			Connection conn = null;
			
			try {
				
				// This is a transaction-wrapped delete */insert *
				conn = csource.createConnection();		
				conn.setAutoCommit(false);
				
				
				{
					String sql = Util.sprintf("DELETE FROM %s", getNtzTableName(_tabName));
					PreparedStatement pstmt = conn.prepareStatement(sql);
					int delrows = pstmt.executeUpdate();
					Util.pf("Deleted %d old rows\n", delrows);
				} 
				
				{
					String sql = Util.sprintf("insert into %s select * from %s",
						getNtzTableName(_tabName), getNtzExtTableName(_tabName));
					PreparedStatement pstmt = conn.prepareStatement(sql);
					int inrows = pstmt.executeUpdate();	
					Util.pf("Inserted %d new rows\n", inrows);
				}
				
				conn.commit();
				
			} catch (SQLException sqlex) {
				
				throw new RuntimeException(sqlex);	
				
			} finally {
				
				if (conn != null)
				{
					try { conn.close(); }
					catch (SQLException sqlex) {}
				}
			}
			
			{
				String dropsql = Util.sprintf("DROP TABLE %s",  getNtzExtTableName(_tabName));
				DbUtil.execSqlUpdate(dropsql, csource);
			}			
			
		}

		// This is important to do to make sure we're not using
		// out-of-date dump files.
		void cleanDumpFile()
		{
			File dfile = new File(getDumpFilePath());
			dfile.delete();
		}
		
		void doDelta() throws Exception
		{
			// Read the most recent dump file 
			initMemFromDumpData();
			
			// Grab data from AdBoard
			renewFromAdBoard();
			
			// Print out delta-info
			showInfo();
			
			// Send the modified records to NTZ
			sendModRecords();
			
			// Clean up, write the ADB data to dump file
			finishClean();
		}
		
		// Okay, load the dump data into the memory store.
		void initMemFromDumpData()
		{
			Util.massert(_dbTabInfo == null,
				"Have leftover table info from previous call, must blow away");
			
			_dbTabInfo = Util.treemap();
			
			List<String> drlist = FileUtils.readFileLinesE(getDumpFilePath());
									
			for(String oneline : drlist)
			{
				DbRecord dumprec = new DbRecord(_colSet, oneline);
				String pkid = getRowPkIdStr(dumprec, _tabName);
				
				Util.massert(!_dbTabInfo.containsKey(pkid),
					"Primary Key %s already in memstore for table %s, probably you have an incorrect PK list, currently %s",
					pkid, _tabName, getIdColList(_tabName));
				
				Util.putNoDup(_dbTabInfo, pkid, dumprec);				
			}
		}
		
		void renewFromAdBoard() throws Exception
		{
			Util.massert(_dbTabInfo != null);
			
			// To start off with, we mark everything as OLD.
			// Most of these will be set to OK below.
			for(DbRecord onerec : _dbTabInfo.values())
				{ onerec.setStatus(RecStatus.oldrec); }

			String selsql = Util.sprintf("SELECT %s FROM %s",  Util.join(_colSet, ","), _tabName);			
			
			SmartAdPuller sap = new SmartAdPuller(selsql, 
				AdBoardPull.getAdBoardRsaPath(),
				Util.getUserName(), AdBoardPull.getAdboardMysqlCreds());
			
			sap.runQuery(false);
			
			// We DO NOT save the dump file here; instead we wait until we are done sending the SQL updates (finishClean method)
			// FileUtils.writeFileLinesE(sap.getOutputLines(), "ADBresponse.txt");
			
			_adbDataList = sap.getOutputLines();
			
			for(String oneline : _adbDataList)
			{
				DbRecord adb_rec = new DbRecord(_colSet, oneline);
				String pkid = getRowPkIdStr(adb_rec, _tabName);
				DbRecord ntz_rec = _dbTabInfo.get(pkid);
								
				// Here, the record does not exist in the current listing
				if(ntz_rec == null) {
					
					adb_rec.setStatus(RecStatus.newrec);
					_dbTabInfo.put(pkid, adb_rec);

				} else if(!adb_rec.equals(ntz_rec)) {
					
					// This is a modification event
					adb_rec.setStatus(RecStatus.modrec);
					_dbTabInfo.put(pkid, adb_rec);
				} else {
				
					// This means there is no change to the record in NTZ
					ntz_rec.setStatus(RecStatus.ok_rec);
				}
			}
		}
		
		boolean showInfo()
		{
			int mdcount = 0;
			Map<RecStatus, Integer> cmap = Util.treemap();			

			for(DbRecord dbrec : _dbTabInfo.values())
			{
				Util.incHitMap(cmap, dbrec.getStatus());
				
				// If OK, no modifications needed. For others - new/mod/old, require an update.
				mdcount += (dbrec.getStatus() == RecStatus.ok_rec ? 0 : 1);
			}
			
			if(mdcount > 0)
			{
				_myLogMail.pf("For table %s found update map: %s\n", _tabName, cmap);
				return true;
				
			} else {
				
				// This is just to give feedback at console that something is going on
				Util.pf("-");
			}
			
			return false;
		}
		
		// TODO: log number of new/modified/sent updates
		void sendModRecords()
		{
			int updcount = 0;
			
			try {
			
				Connection conn = NZConnSource.getNetezzaConn("FASTETL").createConnection();
				
				for(DbRecord onerec : _dbTabInfo.values())
				{
					if(onerec.doDelete())
						{ onerec.deleteFromDb(conn, _tabName); }

					if(onerec.doInsert())
						{ onerec.insert2Db(conn, _tabName); }
					
					// TODO: it's not clear if this status update is ever relevant
					onerec.setStatus(RecStatus.ok_rec);
				}
				
				conn.close();
				
			} catch (SQLException sqlex) {
				
				throw new RuntimeException(sqlex);
			}
		}
		
		void finishClean()
		{
			FileUtils.writeFileLinesE(_adbDataList, getDumpFilePath());
			
			_dbTabInfo.clear();
			_dbTabInfo = null;
		}
	}
	
	static class DbRecord extends TreeMap<String, String>
	{
		private RecStatus _recStat;
		
		DbRecord(Set<String> colset, String sqlresponse)
		{
			String[] toks = sqlresponse.split("\t");
			List<String> collist = new Vector<String>(colset);
			
			Util.massert(toks.length == collist.size(),
				"Found %d tokens, but %d columns", toks.length, collist.size());
			
			for(int i = 0; i < toks.length; i++)
			{
				String inputvalue = (toks[i].equals("NULL") ? null : toks[i]);
				this.put(collist.get(i).toUpperCase(), inputvalue);
			}
		}
		
		DbRecord(Set<String> colset, Map<String, Vector<String>> resmap, int rowid)
		{
			for(String onecol : colset)
			{
				Util.massert(resmap.containsKey(onecol), 
					"Column Name %s not found in result data", onecol);
				
				Util.massert(rowid < resmap.get(onecol).size(),
					"Row ID %d is out of bounds, result count is %d", 
					rowid, resmap.get(onecol).size());
				
				String resvalue = resmap.get(onecol).get(rowid);				
				put(onecol.toUpperCase(), resvalue);
			}
		}		
		
		void setStatus(RecStatus nstat)
		{
			_recStat = nstat;
		}
		
		RecStatus getStatus()
		{
			return _recStat;	
		}
		
		// Delete is true if the record is OLD or MODIFIED
		boolean doDelete()
		{
			return (_recStat == RecStatus.oldrec || _recStat == RecStatus.modrec);
		}
		
		// Do an insert if the record is NEW or MODIFIED
		boolean doInsert()
		{
			return (_recStat == RecStatus.newrec || _recStat == RecStatus.modrec);
		}
		
		public String toString()
		{
			return super.toString();	
		}
		
		public boolean equals(DbRecord that)
		{
			Util.massert(keySet().equals(that.keySet()),
				"Found keyset %s vs %s", 
				keySet(), that.keySet());
			
			//Util.pf("ADB / NTZ : \n\t%s\n\t%s\n", 
			//	toString(), that.toString());
			
			return toString().equals(that.toString());
		}
		
		void deleteFromDb(Connection conn, String tabname) throws SQLException
		{
			List<String> pklist = getIdColList(tabname);
			List<String> wherelist = Util.vector();
			
			for(String onepk : pklist)
			{
				String whcls = Util.sprintf(" %s = ? ", onepk);
				wherelist.add(whcls);
			}
			
			String sql = Util.sprintf(" DELETE FROM %s WHERE %s ",
							getNtzTableName(tabname), Util.join(wherelist, " AND "));
			
			// Util.pf("DEL sql is %s\n", sql);
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			
			for(int i = 0; i < pklist.size(); i++)
			{
				pstmt.setString(i+1, get(pklist.get(i)));
			}
			
			pstmt.executeUpdate();
		}
		
		void insert2Db(Connection conn, String tabname) throws SQLException
		{
			// TODO: going to have to do some kind of PreparedStatement here
			String sql = Util.sprintf("INSERT INTO %s ( %s ) VALUES ( %s )",
							getNtzTableName(tabname), Util.join(keySet(), ","), 
							DbUtil.getNQMarkStr(size()));
			
			Vector<String> inputlist = new Vector<String>(values());
			
			PreparedStatement pstmt = conn.prepareStatement(sql);
			for(int i = 0; i < inputlist.size(); i++)
			{
				pstmt.setString(i+1, inputlist.get(i));
			}			
			pstmt.executeUpdate();
		}
	}
	
		// Gotta 
		/*
		// Not going to use this strategy anymore
		void initFromNtz()
		{
			// This is column name --> List of records
			// will need to transform to build DbRecord
			TreeMap<String, Vector<String>> resmap = Util.treemap();
			
			String selsql = Util.sprintf("SELECT %s FROM %s", 
				Util.join(_colSet, ","), getNtzTableName(_tabName));

			ConnectionSource csource = NZConnSource.getNetezzaConn("FASTETL");

			Util.pf("sql is \n\t%s\n", selsql);

			try {
				Connection conn = csource.createConnection();
				double timesecs = DbUtil.sqlQuery2Map(conn, selsql, resmap);
				
			} catch (SQLException sqlex) {
				throw new RuntimeException(sqlex);
				
			}
			
			int numrec = resmap.firstEntry().getValue().size();
			
			for(int recid = 0; recid < numrec; recid++)
			{
				DbRecord dbrec = new DbRecord(_colSet, resmap, recid);
				String pkid = getRowPkIdStr(dbrec, _tabName);
				_dbTabInfo.put(pkid, dbrec);
				
				// Util.pf("ID is %s for record \n%s\n", id, dbrec);
			}
			
			Util.pf("Found result keyset %s, %d entries\n", 
				resmap.keySet(), resmap.firstEntry().getValue().size());
		}
		*/	
	
	
}
