package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place
import com.adnetik.bm_etl.*; 

/**
 * A "smarter" version of the AdBoardPull, that doesn't pull the entire database.
 * TODO: it seems like this can be made a lot smarter+shorter
 */
public class SmartAdPuller
{		
	String _thesql;
	
	boolean hitError = false;
	
	List<String> _outlines = Util.vector();
	List<String> _errlines = Util.vector();
	
	private String _pathToRsa;
	private String _adboardUser;
	private String[] _adboardMysqlCreds;
	
	private AdBoardMachine _adMachine = AdBoardMachine.prod;
	
	public SmartAdPuller(String sql)
	{
		this(sql, AdBoardPull.getAdBoardRsaPath(), Util.getUserName(), AdBoardPull.getAdboardMysqlCreds());
	}
	
	public SmartAdPuller(String sql, String p2rsa, String username, String[] adbMyCreds)
	{
		Util.massert((new File(p2rsa)).exists(), "RSA file not found %s", p2rsa);
		
		_thesql = sql;
		_pathToRsa = p2rsa;
		_adboardUser = username;
		_adboardMysqlCreds = adbMyCreds;
	}
	
	String getMysqlCommandPref(boolean useheader)
	{
		return Util.sprintf(" mysql -u%s -p%s %s %s ", 
			_adboardMysqlCreds[0], _adboardMysqlCreds[1], 
			(useheader ? "" : "--skip-column-names"), "adnetik");
	}		
	
	
	public void runQuery(boolean useheader) throws Exception
	{
		double startup = Util.curtime();
		int lcount = 0;
		String syscall = Util.sprintf("ssh -i %s %s@%s %s", 
			_pathToRsa, _adboardUser, 
			_adMachine.getHostName(), getMysqlCommandPref(useheader));
		
		// Util.pf("Syscall is \n\t%s\n", syscall);
		
		Process p = Runtime.getRuntime().exec(syscall);
		
		{
			// Write to the process's standard in.
			PrintWriter pwrite = new PrintWriter(p.getOutputStream());	
			pwrite.write(_thesql);
			pwrite.close();
		}
	
		BufferedReader bread = new BufferedReader(new InputStreamReader(p.getInputStream()));
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			_outlines.add(oneline);
		}
		bread.close();
		
		BufferedReader readerr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		for(String oneline = readerr.readLine(); oneline != null; oneline = readerr.readLine())
		{
			_errlines.add(oneline);
		}
		readerr.close();
		
		//Util.pf("Read %d output lines and %d error lines from process, took %.03f secs\n", 
		//	_outlines.size(), _errlines.size(), (Util.curtime()-startup)/1000);

	}
	
	public List<String> getOutputLines()
	{
		return _outlines;	
	}
	
	public List<String> getErrorLines()
	{
		return _errlines;	
	}
}
