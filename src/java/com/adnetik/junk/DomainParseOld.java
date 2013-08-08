
package com.adnetik.bm_etl;

import java.util.*;
import java.io.*;
import java.sql.*;

import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;


public class DomainParseOld
{
	static String[] FIELD_NAMES = new String[] { "campaign_id", "line_item_id", "domain" };
	
	static int DOMAIN_LIMIT = 400;
	
	private Map<Integer, PrintWriter> writerMap = Util.treemap();
	
	Map<String, Integer> exchangeCat;
	Map<String, Integer> domainCat = Util.treemap();
		
	public static void main(String[] args) throws Exception
	{
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		Map<String, String> optargs = Util.getClArgMap(args);
		
		DomainParseOld dparse = new DomainParseOld();
		boolean dolog2shard = true;
		boolean doshard2db = true;
		Integer maxfile = optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : Integer.MAX_VALUE;
		
		if(optargs.containsKey("mode"))
		{ 
			String m = optargs.get("mode");
			dolog2shard = "log2shard".equals(m);
			doshard2db = "shard2db".equals(m);
		}
		
		if(dolog2shard)
		{
			int fCount = 0;
			List<String> pathlist = getLogPaths(daycode);
			Collections.shuffle(pathlist);
			double fstart = Util.curtime();
			
			for(int i = 0; i < pathlist.size() && i < maxfile; i++)
			{
				double startup = Util.curtime();
				String onepath = pathlist.get(i);
				dparse.processLogFile(onepath, daycode);
				double endtime = Util.curtime();
				Util.pf("\nProcessing path %d/%d, took %.03f secs, total %.03f, avg=%.03f/file", 
					i, pathlist.size(), (endtime-startup)/1000, (endtime-fstart)/1000, (endtime-fstart)/(1000*(i+1)));		
			}
			
			dparse.closeWriters();		
		}
		
		if(doshard2db)
		{
			Set<Integer> probeset = Util.treeset();
			
			if(optargs.containsKey("campid"))
				{ probeset.add(Integer.valueOf(optargs.get("campid"))); }
			else
			{ 
				for(int cprobe = 0; cprobe < 10000; cprobe++)
					{ probeset.add(cprobe); }
			}
			
			for(Integer cprobe : probeset)
			{
				File cfile = new File(getCampShardPath(cprobe, daycode));
				
				if(cfile.exists())
				{
					Util.pf("\nFound campaign shard for %s, %d\n", daycode, cprobe);
					CampPack cpack = dparse.getCampPack(cprobe, daycode);
					cpack.process();
				}
			}			
		}
	}
	
	DomainParseOld() throws SQLException
	{
		exchangeCat = DatabaseBridge.readCatalog(DimCode.exchange, false);
		initDomainCatalog();
		
		Util.pf("\nFound %d exchanges, %d domains in catalogs", exchangeCat.size(), domainCat.size());
	}
	
	CampPack getCampPack(int cid, String dayCode)  
	{ 
		return new CampPack(cid, dayCode); 
	}

	void initDomainCatalog() throws SQLException
	{
		Connection conn = DatabaseBridge.getDbConnection(false);
		String sql = "SELECT id, name FROM test_cat_domain";
		PreparedStatement pstmt = conn.prepareStatement(sql);
		ResultSet rset = pstmt.executeQuery();
		
		while(rset.next())
			{ domainCat.put(rset.getString("name"), rset.getInt("id")); }
		
		conn.close();
	}
	
	PrintWriter getMyWriter(int campid, String daycode)
	{
		try {
			String camppath = getCampShardPath(campid, daycode);
			File parentfile = (new File(camppath)).getParentFile();
			parentfile.mkdirs();			
			return new PrintWriter(camppath);	
		} catch (FileNotFoundException ffex) {
			throw new RuntimeException(ffex); 	
		}
	}	
	
	void closeWriters()
	{
		for(PrintWriter pw : writerMap.values())
			{ pw.close(); }
	}
	
	static String getCampShardPath(int campid, String daycode)
	{
		return Util.sprintf("/mnt/data/burfoot/bm_etl/camp_shards/%s/camp_%d.txt", daycode, campid);	
	}
	
	class CampPack
	{
		int campId;
		String dayCode;
		
		LinkedHashMap<String, Integer> topDoms;
		
		CampPack(int cid, String dayc)
		{
			campId = cid;	
			dayCode = dayc;
		}
		
		void transferAll()
		{
			Util.pf("\nTransfering %d TOP domains", topDoms.size());
			
			for(String onedom : topDoms.keySet())
				{ transferDomain(onedom); }
		}		
			
		void process() throws SQLException
		{
			// uploadShardData();
			// findTopDomains();
			popTopDomainTable(DOMAIN_LIMIT);
			// updateDomainCatalog();
			batchTransfer();
		}	
		
		void uploadShardData() throws SQLException
		{
			double startup = Util.curtime();			
			String shardpath = getCampShardPath(campId, dayCode);
			File campfile = new File(shardpath);
			
			Connection conn = DatabaseBridge.getDbConnection(false);
			
			int delrows = DatabaseBridge.execSqlUpdate("DELETE FROM domain_staging", conn);
			int insrows = DatabaseBridge.loadFromFile(campfile, "domain_staging", getColNames(), conn);
			
			conn.close();
			Util.pf("\nDeleted %d rows, inserted %d new rows from domain staging for campid=%d,  took %.03f", 
				delrows, insrows, campId, (Util.curtime()-startup)/1000);
		}		
		
		void popTopDomainTable(int cutoff) throws SQLException
		{
			double startup = Util.curtime();
			Connection conn = DatabaseBridge.getDbConnection(false);
			String delsql = "DELETE FROM top_domains";
			String movsql = "INSERT INTO top_domains (name_domain, numimp) SELECT name_domain, sum(num_impressions) as numimp FROM domain_staging GROUP BY name_domain ORDER BY numimp DESC LIMIT " + cutoff;
			
			int delrows = DatabaseBridge.execSqlUpdate(delsql, conn);
			int movrows = DatabaseBridge.execSqlUpdate(movsql, conn);
			
			conn.close();
			Util.pf("\nFinished pop-top, deleted %d, moved %d, took %.03f", delrows, movrows, (Util.curtime()-startup)/1000);
		}
		
		// Get the top domains in the staging domain database.
		/*
		void findTopDomains() throws SQLException
		{
			topDoms = new LinkedHashMap<String, Integer>();
			
			Connection conn = DatabaseBridge.getDbConnection(false);
			String sql = "SELECT sum(num_impressions) as impcount, name_domain FROM domain_staging GROUP BY name_domain ORDER BY impcount DESC LIMIT " + DOMAIN_LIMIT;
			PreparedStatement pstmt = conn.prepareStatement(sql);
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
				{ topDoms.put(rset.getString(2), rset.getInt(1)); }
			
			conn.close();
			
			Util.pf("\nFound %d top domains for campaign %d", topDoms.size(), campId);
		}	
		*/
		
		/*
		void updateDomainCatalog() throws SQLException
		{
			int newdoms = 0;
			
			for(String maybenew : topDoms.keySet())
			{
				if(!domainCat.containsKey(maybenew))
				{
					int newid = DatabaseBridge.newDomainUpdate(maybenew, "2012-03-15");
					Util.pf("\nFound new domain %s, new id is %d", maybenew, newid);
					domainCat.put(maybenew, newid);
					newdoms++;
				}
			}
			
			Util.pf("\nFinished updating domain catalog, found %d new entries\n", newdoms);
		}		
		*/
		

		// Big INSERT INTO SELECT command for a particular domain
		void transferDomain(String dom)
		{
			int insrows; int delrows;
			{
				List<String> factlist = BmUtil.getFactNames();
				
				// String[] direct = new String[] { "ID_DATE", "ID_CAMPAIGN", "ID_LINEITEM", "ID_EXCHANGE" }
				
				String sql = "INSERT INTO ad_domain";
				sql += Util.sprintf(" ( ID_DATE, ID_CAMPAIGN, ID_LINEITEM, ID_EXCHANGE, ID_DOMAIN , %s ",
					Util.join(factlist, ", "));
				
				sql += ", ENTRY_DATE) ";
				sql += " SELECT ID_DATE, ID_CAMPAIGN, ID_LINEITEM, ID_EXCHANGE, ID_DOMAIN, ";
				for(String onefact : factlist) 
				{
					sql += Util.sprintf(" sum(%s), ", onefact);	
				}
				
				sql += Util.sprintf(" date('%s') ", dayCode);
				sql += " FROM domain_staging_view WHERE NAME_DOMAIN = '" + dom + "' ";
				sql += " GROUP BY ID_DATE, ID_CAMPAIGN, ID_LINEITEM, ID_EXCHANGE, NAME_DOMAIN ";
				
				// Util.pf("SQL is \n%s\n", sql);
				
				insrows = DatabaseBridge.execSqlUpdate(sql, false);
			}
			
			
			{
				String delsql = Util.sprintf("DELETE FROM domain_staging WHERE name_domain = '%s'", 	dom);
				delrows = DatabaseBridge.execSqlUpdate(delsql, false);
			}
			
			// Util.pf("\nInserted %d rows, deleted %d rows for domain %s", delrows, insrows, dom);
		}
		
		// Big INSERT INTO SELECT command for a particular domain
		void batchTransfer()
		{
			double startup = Util.curtime();
			int insrows; int delrows;
			{
				List<String> factlist = BmUtil.getFactNames();
				
				// String[] direct = new String[] { "ID_DATE", "ID_CAMPAIGN", "ID_LINEITEM", "ID_EXCHANGE" }
				
				String sql = "INSERT INTO ad_domain";
				sql += Util.sprintf(" \n\t( ID_DATE, ID_CAMPAIGN, ID_LINEITEM, ID_EXCHANGE, NAME_DOMAIN , \n\t%s ",
					Util.join(factlist, ", "));
				
				sql += Util.sprintf("\n , ENTRY_DATE) SELECT ID_DATE, ID_CAMPAIGN, ID_LINEITEM, ID_EXCHANGE, NAME_DOMAIN, \n", dayCode);
				for(String onefact : factlist) 
				{
					sql += Util.sprintf(" sum(%s), ", onefact);	
				}
				
				sql += Util.sprintf(" date('%s') FROM domain_staging ", dayCode);
				sql += " WHERE NAME_DOMAIN IN \n\t( SELECT name_domain FROM top_domains ) ";
				sql += "\n\tGROUP BY ID_DATE, ID_CAMPAIGN, ID_LINEITEM, ID_EXCHANGE, NAME_DOMAIN ";
				
				Util.pf("SQL is \n%s\n", sql);
				
				insrows = DatabaseBridge.execSqlUpdate(sql, false);
			}
			
			
			{
				String delsql = "DELETE FROM domain_staging WHERE name_domain IN  ( SELECT name_domain FROM top_domains )";
				//delrows = DatabaseBridge.execSqlUpdate(delsql, false);
				delrows = 0;
			}
			
			Util.pf("\nDone with batch transfer, inserted %d rows into ad_domain, deleted %d rows, took %.03f secs", insrows, delrows, (Util.curtime()-startup)/1000);
		}	
		
		private String getSubSelect(int cutoff)
		{
			return "SELECT id_domain FROM domain_staging_view GROUP BY id_domain having sum(num_impressions) >= " + cutoff;
		}
	}
	
	void processLogFile(String fpath, String daycode) throws IOException
	{
		int lcount = 0;
		BufferedReader bread = Util.getGzipReader(fpath);
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			BleStructure bles = null; 
			
			try { bles = BleStructure.buildStructure(LogType.imp, LogVersion.v14, oneline); }
			catch (BidLogFormatException blex) { continue; }
				
			List<String> dlist = Util.vector();
						
			for(String fname : FIELD_NAMES)
				{ dlist.add(bles.logentry.getField(fname)); }

			// Need to use special lookup logic here
			{
				String excname = bles.logentry.getField("ad_exchange");
				dlist.add(exchangeCat.get(excname)+"");
			}
			
			Metrics onemet = bles.returnMetrics();
			
			int campid = bles.logentry.getIntField("campaign_id");
			
			if(!writerMap.containsKey(campid))
			{	 
				writerMap.put(campid, getMyWriter(campid, daycode));
			}
			
			PrintWriter cwrite = writerMap.get(campid);
			
			cwrite.write(Util.join(dlist, "\t"));
			cwrite.write("\t");
			cwrite.write(onemet.toString("\t", false));
			cwrite.write("\n");			
			
			lcount++;
		}
		
		bread.close();
		Util.pf("\nWrote %d lines for file \n\t%s", lcount, fpath);
	}

	static List<String> getColNames()
	{
		List<String> clist = Util.vector();
		clist.add("ID_CAMPAIGN");
		clist.add("ID_LINEITEM");
		clist.add("NAME_DOMAIN");
		clist.add("ID_EXCHANGE");		
		
		for(IntFact intf : IntFact.values())
			{ clist.add("NUM_" + intf.toString().toUpperCase()); }

		for(DblFact dblf : DblFact.values())
			{ clist.add("IMP_" + dblf.toString().toUpperCase()); }
		
		return clist;
	}
	
	static List<String> getLogPaths(String daycode)
	{
		List<String> plist = Util.vector();
		
		for(ExcName exc : ExcName.values())
		{
			List<String> sublist = Util.getNfsLogPaths(exc, LogType.imp, daycode);
			
			if(sublist == null)
				{ Util.pf("\nWarning, no log files found for excname %s, daycode=%s", exc, daycode); }
			else 
				{ plist.addAll(sublist); }
		}

		return plist;
	}
	

}
