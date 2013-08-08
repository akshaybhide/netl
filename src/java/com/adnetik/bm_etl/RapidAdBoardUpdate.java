package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

public class RapidAdBoardUpdate
{
	private SimpleMail _logMail;
	private DbTarget _dbTarg;
	private String _dayCode;
	
	// Names of columns on REPORTING DB, not UI
	private Map<TableName, List<String>> _tableColMap = Util.treemap();
	private Map<TableName, Map<String, String>> _bigRepMap = Util.treemap();
	
	// List of tables to update 
	public enum TableName { 
		user_report_access, group_report_access,
		campaign, campaign_status, creative, line_item, office, user, account, 
		assignment, client, 
		agency,
		publisher, site, placement_group, placement,
		external_line_item, external_campaign,
		cost, targeting_list, 
		campaign_activity_pixel, // pixel_type
		pixel, pixel_param // time_window_ranges TODO,
	};
	
	// private static final String REPORTING_DB_NAME = "adnetik_test_08_29";
	private static final String REPORTING_DB_NAME = "adnetik";
	private static final String UI_DB_NAME = "adnetik";
	
	private static final DbTarget _CUR_TARGET = DbTarget.internal;
	
	public static void main(String[] args) throws Exception
	{
		ArgMap argmap = Util.getClArgMap(args);
		
		String dbtargstr = argmap.getString("dbtarget", DbTarget.internal.toString());
		
		RapidAdBoardUpdate rapup = new RapidAdBoardUpdate(DbTarget.valueOf(dbtargstr));	
		rapup.runUpdates();
	}
	
	public RapidAdBoardUpdate(DbTarget dbt)
	{
		_dayCode = TimeUtil.getTodayCode();
		_logMail = new SimpleMail("RapidAdBoardUpdate Log for " + _dayCode);
		_dbTarg = dbt;
		
		for(TableName tname : TableName.values())
		{
			// _logMail.pf("Querying column names for table %s\n", tabname);
			String showsql = Util.sprintf("DESCRIBE %s.%s", REPORTING_DB_NAME, tname);
			List<String> colnames = DbUtil.execSqlQuery(showsql, new DatabaseBridge(_dbTarg));
						
			_logMail.pf("Found %d columns for table %s\n", colnames.size(), tname);
			_tableColMap.put(tname, colnames);	
		}
	}
	
	void runUpdates() throws Exception
	{
		for(int i : Util.range(24))
		{
			for(int hr : Util.range(4))
			{
				_logMail.pf("--------------------------------------------\n");
				_logMail.pf("Running update for hour=%d, quarter=%d\n", i, hr*15);
				
				if(!_dayCode.equals(TimeUtil.getTodayCode()))
					{ break; }
				
				runSingleUpdate();
				
				// Sleep for 15 minutes
				Thread.sleep(1000*60*15);
				// Thread.sleep(1000*10);
			}
		}
		
		_logMail.send2admin();		
	}
	
	void runSingleUpdate() throws Exception
	{
		List<String> allrep = Util.vector();
		
		allrep.add("BEGIN;");
		
		// These tables are DELETED every time
		for(TableName tname : new TableName[] { TableName.user_report_access, TableName.group_report_access })
		{
			// String delsql = Util.sprintf("DELETE FROM %s.%s ;", UI_DB_NAME, tname);
			// allrep.add(delsql);			
		}
		
		
		for(TableName onetab : TableName.values())
		{		
			List<String> curtabdata = grabTableData(onetab);
			// doBounceUp(targname, newtabdata);
			Map<String, String> repmap = getReplaceSqlMap(onetab, curtabdata);

			List<String> updates = findAndUpdateReps(onetab, repmap);
			allrep.addAll(updates);
		}	
		
		
		allrep.add("COMMIT;");
		
		// push the replace statements up
		sendReplaceInfo(allrep);

		// randomPerturbMemInfo();
	}
	

	
	// Get the CHAINED Mysql command that can be written to through STDIN
	// eg. ssh -i <private key file> burfoot@<hostname> mysql -u burfoot -h localhost
	private String getChainSshMysqlCall() 
	{
		String mysqlcall = AdBoardPull.getMysqlPushCall(_dbTarg);
		
		Util.SshUtil sshut = new Util.SshUtil();
		{
			String[] host_db = DatabaseBridge.getIpDbName(_dbTarg);		
			sshut.hostname = host_db[0];
			sshut.username = "burfoot";
			sshut.rsapath = AdBoardPull.BURFOOT_RSA_PATH;		
		}
		
		return sshut.getSysCall(mysqlcall);
	}
	
	private void sendReplaceInfo(List<String> replist) throws IOException
	{
		/*
		for(int i : Util.range(replist.size()))
		{
			Util.pf("Statement %d = %s\n", i, replist.get(i));
		}
		*/
		
		if(replist.isEmpty())
		{
			_logMail.pf("No updates for this block, returning\n");
			return; 
		}
		
		_logMail.pf("Going to send %d replace statements\n", replist.size());
		
		String syscall = getChainSshMysqlCall();

		Util.pf("Syscall is \n\t%s\n", syscall);			
		// for(int i : Util.range(4))
		// { Util.pf("Replace command %s\n", replist.get(i));		
		
				
		List<String> outlist = Util.vector();
		List<String> errlist = Util.vector();
		replist.add("SHOW WARNINGS;");
		
		double startup = Util.curtime();
		boolean haderr = Util.syscall(syscall, replist, outlist, errlist);
		_logMail.pf("Sent %d lines total, took %.03f secs\n", replist.size(), (Util.curtime()-startup)/1000);	
		
		if(haderr)
		{
			// TODO: is this the correct thing to do here?
			for(String oneerr : errlist)
				{ _logMail.pf("ERROR: %s\n", oneerr);	}	
			
			_logMail.send2admin();
			throw new RuntimeException("ERROR IN UPLOAD");
		}				
	}
	
	List<String> findAndUpdateReps(TableName onetab, Map<String, String> newmap)
	{
		Util.setdefault(_bigRepMap, onetab, new TreeMap<String, String>());
		Map<String, String> curmap = _bigRepMap.get(onetab);		
		
		List<String> replist = Util.vector();
		int updates = 0;
		int newrows = 0;
				
		for(String oneid : newmap.keySet())
		{
			String currep = curmap.get(oneid);
			String newrep = newmap.get(oneid);
			
			if(!newrep.equals(currep))
			{
				if(currep == null) 
				{
					// Util.pf("Found new entry for table %s, ID %d\n", onetab, oneid);
					newrows++;
				} else {
					_logMail.pf("Found an update for table %s, ID %s\n", onetab, oneid);
					updates++;					
				}
				
				replist.add(newrep);	
				curmap.put(oneid, newrep);
			}
		}

		if(updates + newrows > 0)
			{ _logMail.pf("Found %d updates, %d new rows for table %s\n", updates, newrows, onetab); }
			
		return replist;
	}
	
	List<String> grabTableData(TableName mytab) throws Exception
	{
		// Util.pf("Grabbing table data for table %s\n", targtabname);
		
		List<String> colnames = _tableColMap.get(mytab);
		String selsql = Util.sprintf("SELECT %s FROM %s.%s", Util.join(colnames, ","), UI_DB_NAME, mytab);
		
		Util.pf("SELECT SQL \n\t%s\n", selsql);
		
		SmartAdPuller sap = new SmartAdPuller(selsql);
		sap.runQuery(true);	
		
		if(!sap.getErrorLines().isEmpty())
		{
			for(String errline : sap.getErrorLines())
				{ System.err.printf("AdPull error: %s\n", errline);	}
			
			throw new RuntimeException("Error in SmartAdPull: " + sap.getErrorLines().get(0));
		}
		
		// Vague feeling that I should copy result of SAP runQuery, instead of returning reference to it
		List<String> outlines = new Vector<String>(sap.getOutputLines());
		return outlines;
	}
	
	private String[] getIdColList(TableName tname)
	{
		if(tname == TableName.campaign_activity_pixel)
			{ return new String[] { "campaign_id", "activity_pixel_id" }; }
		
		if(tname == TableName.user_report_access)
			{ return new String[] { "user_id", "user_group_id", "cost" }; }		
		
		if(tname == TableName.group_report_access)
			{ return new String[] { "user_group_id", "account_id" }; }			
		
		return new String[] { "id" }; 
	}
	
	Map<String, String> getReplaceSqlMap(TableName mytab, List<String> saplines)
	{
		Map<String, String> repsqlmap = Util.treemap();
		String[] fnames = saplines.get(0).split("\t");
		String[] qmarks = new String[fnames.length];
		Arrays.fill(qmarks, "?");
		
		// The index of the id field in the field names
		String[] idcollist = getIdColList(mytab);
		Util.massert(idcollist.length > 0);
		
		Set<Integer> idindset = Util.treeset();
		for(String idcol : idcollist)
		{
			int id_ind = Arrays.asList(fnames).indexOf(idcol);
			Util.massert(id_ind > -1, "Could not find id field %s", idcol);
			idindset.add(id_ind);
		}
		Util.massert(idindset.size() == idcollist.length);
		
		
		try { 
			
			Connection conn = DatabaseBridge.getDbConnection(_dbTarg);	
			String repinto = Util.sprintf("REPLACE INTO %s.%s (%s) VALUES ( %s ) ;", 
				REPORTING_DB_NAME, mytab, Util.join(fnames, ","), Util.join(qmarks, ","));
	
			PreparedStatement pstmt = conn.prepareStatement(repinto);
			
			
			// Start at one b/c of mysql header
			for(int i = 1; i < saplines.size(); i++)
			{
				String[] vals = saplines.get(i).split("\t");
				
				for(int j = 0; j < vals.length; j++)
				{ 
					pstmt.setString(j+1, vals[j].equals("NULL") ? null : vals[j]); 
				}
				
				String nativesql = pstmt.toString();
				int colonind = nativesql.indexOf(":");
				nativesql = nativesql.substring(colonind+1).trim();
				
				try {
					String combkey = getCombKey(idindset, vals);
					
					// Integer id = Integer.valueOf(vals[id_ind]);
					// Util.pf("Native sql is:\n%s\n", nativesql);
					Util.putNoDup(repsqlmap, combkey, nativesql);
					// repsqlmap.put(id, nativesql);
				} catch (NumberFormatException nfex) {
					
					// This is usually caused by having weird strings in a column,
					// must remove things like appnexus_message
					Util.pf("Bad SQL response line: %s\n", saplines.get(i));	
					Util.pf("Prev line is %s\n", saplines.get(i-1));
					Util.pf("Next line is %s\n", saplines.get(i+1));
					throw new RuntimeException(nfex);
				}
			}
						
			conn.close();
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
		}	
		
		return repsqlmap;
	}
	
	private String getCombKey(Set<Integer> idset, String[] vals)
	{
		List<Integer> idlist = Util.vector();
		for(Integer idcol : idset)
		{
			Integer oneid = Integer.valueOf(vals[idcol]);
			idlist.add(oneid);
		}
		
		return Util.join(idlist, Util.DUMB_SEP); 
	}
	
	private void randomPerturbMemInfo()
	{
		List<String> tweaklist = Util.vector();
		
		for(TableName onetab : _bigRepMap.keySet())
		{
			for(String id : _bigRepMap.get(onetab).keySet())
			{
				if(Math.random() < .0001)
				{
					// Util.pf("Randomly tweaking id %d\n", id);
					tweaklist.add(id);
					_bigRepMap.get(onetab).put(id, "JUNK STRING");
				}
			}
		}
		
		Util.pf("Randomly tweaking ids: %s\n", tweaklist);
	}
}
