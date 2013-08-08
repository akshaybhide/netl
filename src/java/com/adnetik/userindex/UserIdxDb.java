
package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.sql.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.DbUtil.*;

import com.adnetik.shared.*;


import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

// Database logic related to User Index
public class UserIdxDb extends DbUtil.SimpleSource
{	
	public static final String LIFT_DUMP_FILE = UserIndexUtil.LOCAL_UINDEX_DIR + "/db_temp/LIFT_DUMP.tsv";
	public static final String EVAL_DUMP_FILE = UserIndexUtil.LOCAL_UINDEX_DIR + "/db_temp/EVAL_DUMP.tsv";
	
	
	public enum AdaClassCol { report_idInt, feat_nameStr, name_keyStr, weightDbl };
	
	UserIdxDb()
	{
		super(AidxRelDb.UserIndexMain);	
	}

	public static Connection getConnection() throws SQLException
	{
		return (new UserIdxDb()).createConnection();
	}

	
	static int loadEvalData()
	{
		List<String> colnames = Util.vector();
		colnames.add("REPORT_ID");
		colnames.add("USER_RANK");
		colnames.add("USER_SCORE");
		
		File loadfrom = new File(EVAL_DUMP_FILE);
		return loadDumpFile(colnames, "eval_scheme", loadfrom);		
		
	}
	
	static int loadLiftReport()
	{
		List<String> colnames = Util.vector();
		colnames.add("REPORT_ID");
		colnames.add("USER_RANK");
		colnames.add("USER_SCORE");
		
		File loadfrom = new File(LIFT_DUMP_FILE);
		return loadDumpFile(colnames, "lift_report", loadfrom);
	}

	private static int loadDumpFile(List<String> colnames, String tabname, File loadfrom)
	{
		try { 
			Connection conn = getConnection();	
			int numup = DbUtil.loadFromFile(loadfrom, tabname, colnames, conn);
			conn.close();
			return numup;
			
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex);	
		}		
	}
		
	public static void deleteReportId(int reportid)
	{
		// report_info has to come last
		String[] reftables = new String[] { "eval_scheme", "lift_report", "feature_table", "report_info" };
		
		for(String onetab : reftables)
		{
			deleteOld(reportid, onetab);
		}
	}
	
	static int deleteOld(int reportid, String tabname)
	{
		String delsql = Util.sprintf("delete from %s where report_id = %d", tabname, reportid);
		
		try { 
			Connection conn = getConnection();
			int delrows = DbUtil.execSqlUpdate(delsql, conn);
			conn.close();
			return delrows;
			
		} catch (SQLException sqlex) {
				
			throw new RuntimeException(sqlex);
		}
	}
	
	static int lookupCreateRepId(String blockend, PosRequest posreq)
	{
		Integer lookup = lookupReportId(blockend, posreq);
		
		if(lookup != null)
			{ return lookup; }
		
		NegRequest negreq = posreq.getNegRequest();
		
		try {
			Connection conn = getConnection();
			String in_sql = Util.sprintf("INSERT INTO report_info (can_day, pos_list_code, neg_list_code, old_list_code) VALUES ('%s', '%s', '%s', '%s')",
				blockend, posreq.getListCode(), negreq.getListCode(), posreq.getOldListCode());
			DbUtil.execSqlUpdate(in_sql, conn);
			conn.close();

		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex); 			
		}

		return lookupReportId(blockend, posreq);
	}
	
	static Integer lookupReportId(String blockend, PosRequest posreq)
	{
		// Only block-end dates have report_ids
		UserIndexUtil.assertValidBlockEnd(blockend);
		
		List<Integer> replist = null;
		
		try {
			Connection conn = getConnection();
			
			String lookupsql = Util.sprintf("SELECT report_id FROM report_info WHERE can_day = '%s' AND pos_list_code = '%s'",
				blockend, posreq.getListCode());
			
			replist = DbUtil.execSqlQuery(lookupsql, conn);		
			conn.close();
			
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex); 
		}
		
		Util.massert(replist.size() <= 1, 
			"Too many entries in report info table for daycode %s, poscode %s", blockend, posreq.getListCode());
		
		return replist.isEmpty() ? null : replist.get(0);
	}
	
	// This just inserts a skeleton record; then call ScanRequest.persist2db()
	static void insertListCodeShell(String listcode, String country, String entrydate)
	{
		String sql = Util.sprintf("INSERT INTO listen_code (listcode, country, entry_date) VALUES ('%s', '%s', '%s')", 
			listcode, country, entrydate);
		DbUtil.execSqlUpdate(sql, new UserIdxDb());
	}
	
	static List<String>  getListenCodeList()
	{
		String sql = "SELECT listcode FROM listen_codes";
		return DbUtil.execSqlQuery(sql, new UserIdxDb());
	}
	
	static List<Pair<String, String>> getPrmpxKeyVal(String listcode)
	{
		String sql = Util.sprintf("SELECT param_key, param_val FROM list_pxprm_str WHERE listcode = '%s'", listcode);
		return DbUtil.execSqlQueryPair(sql, new UserIdxDb());
	}
	
	private static void spoolToUserDb(String listcode, String canday, BufferedReader bread) throws IOException
	{
		List<String> fieldnames = Util.vector();
		{
			fieldnames.add("listcode");
			fieldnames.add("canday");
			for(LogField fname : FieldLookup.getFieldList(LogType.UIndexMinType, LogVersion.UIndexMinVers2))
				{ fieldnames.add(fname.toString()); }
		}
		
		List<String> linelist = Util.vector();
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String[] toks = oneline.split("\t");
			linelist.add(Util.sprintf("%s\t%s\t%s", listcode, canday, Util.joinButFirst(toks, "\t")));
		}
		
		FileUtils.writeFileLinesE(linelist, "inf_file.txt");
		int uprows = DbUtil.loadFromFile(new File("inf_file.txt"), "user_activity", fieldnames, new UserIdxDb());
		Util.pf("Loaded %d rows into database\n", uprows);		
	}
	
	static void uploadClassifData(Map<PosRequest, AdaBoost<UserPack>> classifmap, String blockend) throws IOException
	{
		String infpath = Util.sprintf("%s/inf_%s.txt", UserIndexUtil.LOCAL_DBTEMP_DIR, blockend);
		InfSpooler infspool = new InfSpooler(AdaClassCol.values(), infpath, new UserIdxDb(), "adaclass_info");
		
		try {
			Connection conn = (new UserIdxDb()).createConnection();
			
			PreparedStatement pstmt = conn.prepareStatement("INSERT INTO adaclass_info (report_id, name_key, feat_name, weight) values (? , ?, ? , ?)");
			
			for(PosRequest posreq : classifmap.keySet())
			{
				int repid = lookupCreateRepId(blockend, posreq);
				
				{
					String delsql = Util.sprintf("DELETE FROM adaclass_info WHERE report_id = %d", repid);
					int delrows = DbUtil.execSqlUpdate(delsql, new UserIdxDb());
					
					if(delrows > 0)
						{ Util.pf("%d old rows deleted for report_id %d\n", delrows, repid); }
				}
				
				AdaBoost<UserPack> classif = classifmap.get(posreq);
				
				Util.pf("Report ID for poscode %s, blockend %s is %d\n",
					posreq.getListCode(), blockend, repid);
				
				for(int t = 0; t < classif.numFuncs(); t++)
				{
					String namekey = classif.getBaseNameKey(t);
					double alpha = classif.getAlphaVal(t);
					
					if(classif.isReverse(t))
						{ alpha = -alpha; }
					
					UserFeature ufeat = UserFeature.buildFromNameKey(namekey);
					
					pstmt.setInt(1, repid);
					pstmt.setString(2, namekey);
					pstmt.setString(3, ufeat.toString());
					pstmt.setDouble(4, alpha);
					pstmt.executeUpdate();
				}
			}
			
			conn.close();
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);	
		}
	}
	
	public static class Analyzer
	{
		int _reportId;
		
		int _liftTotal;
		int _evalTotal; 
		double _totalRatio;
		
		
		private boolean _hasData = true;
		
		public Analyzer(int reportid)
		{
			_reportId = reportid;
			
			checkData();
			
			if(_hasData)
			{
				_liftTotal = getGenCount(true, -10000);
				_evalTotal = getGenCount(false, -10000);	
				
				// Should be much > 1
				_totalRatio = ((double) _liftTotal)/(_evalTotal);
			}
			

		}
		
		public boolean hasData()
		{
			return _hasData;
		}
		
		private String getViewName(boolean  islift)
		{
			return (islift ? "v__full_lift" : "v__full_eval");
		}
		
		private void checkData()
		{			
			for(boolean islift : new boolean[] { true, false })
			{
				String sql = Util.sprintf("SELECT count(*) FROM %s WHERE report_id = %d",
					getViewName(islift), _reportId);	

				List<Long> reslist = DbUtil.execSqlQuery(sql, new UserIdxDb());		
				
				_hasData &= (reslist.get(0) > 40);
			}
		}
		
		
		private int getGenCount(boolean islift, double cutoff)
		{
			String sql = Util.sprintf("SELECT MAX(user_rank) FROM %s WHERE user_score > %.03f AND report_id = %d",
				(islift ? "v__full_lift" : "v__full_eval"), cutoff, _reportId);
			
			List<Integer> rlist = DbUtil.execSqlQuery(sql, new UserIdxDb());
			
			if (rlist == null || rlist.get(0) == null)
				{ return 0; }
			
			return rlist.get(0);
		}

		// Cutoff such that # users > cutoff = targnum
		public double cutoff4LiftCount(int targnum)
		{
			String sql = Util.sprintf("SELECT max(user_score) from v__full_lift where report_id = %d and user_rank > %d",
				_reportId, targnum);
			
			List<Double> reslist = DbUtil.execSqlQuery(sql, new UserIdxDb());
			return reslist.get(0);
		}
		
		public double evalFracAbove(double cutoff)
		{
			double numabove = getGenCount(false, cutoff);
			return numabove/_evalTotal; 
		}
		
		public double liftFracAbove(double cutoff)
		{
			double numabove = getGenCount(true, cutoff);
			return numabove/_liftTotal; 			
		}
		
		public Double getMultiplier(double cutoff)
		{
			double numlift = getGenCount(true, cutoff);
			double numeval = getGenCount(false, cutoff);
			
			if(numlift == 0)
				{ return null; }
			
			return (numeval/numlift) * _totalRatio;
		}
		
		public int getTotalLiftUserCount()
		{
			return _liftTotal;
		}
		
		public int getTotalEvalUserCount()
		{
			return _evalTotal;
		}		
		
		public SortedSet<Pair<Double, Integer>> getMult2TargMap()
		{
			SortedSet<Pair<Double, Integer>> cset = new TreeSet<Pair<Double, Integer>>(Collections.reverseOrder());
			
			int hk = 100000;
			int m = 1000000;
			
			int[] targsize = new int[] { hk, 3*hk, 6*hk, 8*hk, 10*hk, 13*hk, 15*hk, 20*hk, 25*hk };		
			for(int onet : targsize)
			{
				Double cutoff = cutoff4LiftCount(onet);
				Double mult = getMultiplier(cutoff);
				
				if(mult == null)
					{ continue; }
				
				cset.add(Pair.build(mult, onet));
			}
			
			return cset;
		}		
		
		
		public SortedSet<Pair<Double, Double>> getMultiplierMap()
		{
			SortedSet<Pair<Double, Double>> cset = new TreeSet<Pair<Double, Double>>(Collections.reverseOrder());
			double[] cutoffs = new double[] { -.3, -2.5, -2, -1.5, -1.0, -0.5, 0, 0.5, 1.0, 1.5, 2.0 };
			
			for(double onec : cutoffs)
			{
				Double mult = getMultiplier(onec);
				if(mult == null)
					{ continue; }
				
				cset.add(Pair.build(mult, onec));
			}
			
			return cset;
		}
	}
	
	public static void main(String[] args)
	{
		
		if("examineone".equals(args[0]))
		{
			int reportid = Integer.valueOf(args[1]);
			
			Analyzer lyzer = new Analyzer(reportid);
			
			SortedSet<Pair<Double,Double>> multmap = lyzer.getMultiplierMap();
			
			for(Pair<Double, Double> onepair : multmap)
			{
				Util.pf("Found multiplier %.03f for cutoff %.03f\n", onepair._2, onepair._1);	
			}
		} 
		
		if("examinesize".equals(args[0]))
		{
			int reportid = Integer.valueOf(args[1]);
			
			Analyzer lyzer = new Analyzer(reportid);
			
			if(!lyzer.hasData())
			{ 
				Util.pf("No data found for reportid=%d\n", reportid); 
				return;
			}
			
			
			SortedSet<Pair<Double,Integer>> multmap = lyzer.getMult2TargMap();
			
			for(Pair<Double, Integer> onepair : multmap)
			{
				Util.pf("Found multiplier %.03f for targsize %d\n", onepair._1, onepair._2);	
			}
		} 		
		
		if("showseveral".equals(args[0]))
		{
			for(int reportid = 600; reportid < 620; reportid++)
			{
				Analyzer lyzer = new Analyzer(reportid);
				double cutoff = 0.0D;
				
				if(!lyzer.hasData())
				{
					Util.pf("No data found for reportid %d\n", reportid);
					continue; 
				}
				
				int totlcount = lyzer.getTotalLiftUserCount();
				int evalcount = lyzer.getTotalEvalUserCount();
				double evalfrac = lyzer.evalFracAbove(cutoff);
				double liftfrac = lyzer.liftFracAbove(cutoff);
				
				Util.pf("Found eval=%d, totl=%d for reportid=%d\n", evalcount, totlcount, reportid);
				Util.pf("Eval/lift frac for %.03f is %.03f/%.03f\n", cutoff, evalfrac, liftfrac);			
			}			
		}
		
		if("showcutoff".equals(args[0]))
		{
			for(int reportid = 600; reportid < 620; reportid++)
			{
				Analyzer lyzer = new Analyzer(reportid);
				int targnum = 2000000;
				
				if(!lyzer.hasData())
				{
					Util.pf("No data found for reportid %d\n", reportid);
					continue; 
				}
				
				double cutoff = lyzer.cutoff4LiftCount(targnum);
				
				int totlcount = lyzer.getTotalLiftUserCount();
				int evalcount = lyzer.getTotalEvalUserCount();
				double evalfrac = lyzer.evalFracAbove(cutoff);
				double liftfrac = lyzer.liftFracAbove(cutoff);
				
				Util.pf("Found eval=%d, totl=%d for reportid=%d\n", evalcount, totlcount, reportid);
				Util.pf("Eval/lift frac for %.03f is %.03f/%.03f\n", cutoff, evalfrac, liftfrac);			
			}			
		}	
		
	}
}
