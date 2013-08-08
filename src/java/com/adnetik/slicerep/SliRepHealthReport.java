
package com.adnetik.slicerep;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import java.math.BigInteger;

public class SliRepHealthReport
{
	private String _dayCode;
	private SimpleMail _logMail;
		
	public static void main(String[] args)
	{
		SliRepHealthReport hmm = new SliRepHealthReport();
		hmm.runCheck();
		/*
		Util.pf("Health monitor mail\n");
		LogWrangler lwrang = new LogWrangler("2012-07-31");
		
		Util.pf("Found %d log stat files\n", lwrang.getNumFiles());
		lwrang.slurpData();
		lwrang.writeCsv("my2.csv");
		*/
	}

	public SliRepHealthReport()
	{
		_dayCode = TimeUtil.getYesterdayCode();
		_logMail = new SimpleMail("SliRepHealthReport for " + _dayCode);
	}
	
	void runCheck()
	{
		checkCleanList();	
		
		databaseSizeCheck();
		
		_logMail.send2admin();
	}
	
	void databaseSizeCheck()
	{
		String sql = "";
		sql += "SELECT CONCAT(table_schema, '.', table_name) as tabname,";
		sql += "CONCAT(ROUND(table_rows / 1000000, 2), 'M') as rows, ";
		sql += "CONCAT(ROUND(data_length / ( 1024 * 1024 * 1024 ), 2), 'G') as datalength, ";
		sql += "CONCAT(ROUND(index_length / ( 1024 * 1024 * 1024 ), 2), 'G') as idxlength, ";
		sql += "CONCAT(ROUND(( data_length + index_length ) / ( 1024 * 1024 * 1024 ), 2), 'G') total_size, ";
		sql += "ROUND(index_length / data_length, 2) as idxfrac ";
		sql += "FROM information_schema.TABLES ORDER  BY datalength + idxlength DESC LIMIT 10";

		// Util.pf("SQL is:\n%s\n", sql);
		
		String[] colnames = new String[] { "rows", "datalength", "idxlength", "total_size", "idxfrac" };
		
		try {
			Connection conn = SliDatabase.getConnection();
			PreparedStatement pstmt = conn.prepareStatement(sql);
			
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				String tabname = rset.getString("tabname");
				
				StringBuffer sb = new StringBuffer();
				sb.append("For table " + tabname + " \n");
				
				for(String colname : colnames)
				{
					sb.append(Util.sprintf("\t%s=%s", colname, rset.getString(colname)));	
				}
				
				_logMail.pf(sb.toString() + "\n");
			
			}
		}  catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
		}
		
		
	}
	
	void checkCleanList()
	{	
		String cleanpath = SliUtil.getCleanListPath(_dayCode);
		Util.pf("Going to check the clean list, path is %s..\n", cleanpath);
		
		Set<String> cleanset = new TreeSet<String>(FileUtils.readFileLinesE(cleanpath));
		Util.pf("Found %d paths in cleanlist \n", cleanset.size());
		
		Set<String> allpathset = SliUtil.getPathsForDay(_dayCode);
		
		int okcount = 0;
		int misscount = 0;
		
		for(String onepath : allpathset)
		{
			if(cleanset.contains(onepath))
			{ 
				okcount++; 
			} else {
				
				if(misscount < 10)
					{ _logMail.pf("WARRRRNNNNINNNNGGGG: path %s not found in clean list !!!!!\n", onepath); }
				
				misscount++;
			}
		}
		
		if(misscount == 0)
			{ _logMail.pf("Success, all %d NFS paths found in clean list\n", cleanset.size()); }
		else {
			_logMail.pf("Problems detected in clean list: %d out of %d total paths found\n",
				okcount, allpathset.size());
		}
	}

	public static class LogWrangler implements FileFilter
	{
		String _dayCode;
		SortedMap<String, String[]> _csvData = Util.treemap();
		
		public LogWrangler(String dc)
		{
			_dayCode = dc;
		}
		
		public boolean accept(File targfile)
		{
			String sname = targfile.getName();
			return sname.startsWith("log") && sname.endsWith(".txt");
		}
		
		void slurpData()
		{
			File[] statfiles = getStatFileList();
			
			for(int i = 0; i < statfiles.length; i++)
			{
				List<String> statlines = FileUtils.readFileLinesE(statfiles[i].toString());
				
				for(String oneline : statlines)
				{
					String[] k_v = oneline.split("\t");
					Util.setdefault(_csvData, k_v[0], new String[statfiles.length]);
					_csvData.get(k_v[0])[i] = k_v[1].replace(",", "");
				}
			}
			
			Util.pf("Found key set %s\n", _csvData.keySet());	
		}
		
		void writeCsv(String writepath)
		{
			String delim = ",";
			
			List<String> csvlines = Util.vector();
			
			{
				List<String> header = new Vector<String>(_csvData.keySet());	
				csvlines.add(Util.join(header, delim));
			}
			
			for(int i = 0; i < _csvData.get(_csvData.firstKey()).length; i++)
			{
				List<String> onerow = Util.vector();
				for(String key : _csvData.keySet())
				{ 
					String toadd = (_csvData.get(key)[i] == null ? "" : _csvData.get(key)[i]);
					onerow.add(toadd);
				}
				
				// Util.pf("Onerow has %d elements\n", onerow.size());
				// Util.pf("Onerow is %s\n", Util.join(onerow, ","));
				csvlines.add(Util.join(onerow, delim));
			}
			
			FileUtils.writeFileLinesE(csvlines, writepath);
		}
		
		public int getNumFiles()
		{
			return getStatFileList().length;
		}
		
		private File[] getStatFileList()
		{
			File statdir = new File(SliUtil.getLogStatsDir(_dayCode));
			Util.massert(statdir.exists(), "Stats dir does not exist  %s", statdir);
			Util.massert(statdir.isDirectory(), "Stats file is not a directory %s", statdir);
			return statdir.listFiles(this);
		}
	}
}
