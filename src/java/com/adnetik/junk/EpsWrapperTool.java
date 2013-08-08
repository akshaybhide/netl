
package com.adnetik.analytics;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

// Eventually, this should just be a wrapper around some kind of database call.
public class EpsWrapperTool
{
	public static String TRK_SLICE_CACHE_DIR = "/tmp/burfoot/TRK_SLICE_CACHE";
	
	
	Map<String, String> optMap = Util.treemap();
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = (new EpsWrapperTool()).run(args);
		System.exit(exitCode);
	}
		

	
	public int run(String[] args) throws Exception
	{				
		{
			optMap.put("outputeps", "/mnt/burfoot/epstest/TestData.eps");
			optMap.put("daycode", "2011-10-02");
			optMap.put("campid", "all");
			optMap.put("viztype", "imps_per_user");
			Util.putClArgs(args, optMap);
		}
		
		String daycode = optMap.get("daycode");
		String campid = optMap.get("campid");
		
		Util.pf("\nRunning EPS wrapper for \n\tdaycode=%s \n\tcampid=%s \n\toutputeps=%s", daycode, campid, optMap.get("outputeps"));
		
		//LinkedList<String> datalines = getTrackSlice(daycode);
		//List<UserPack> userlist = aggData(datalines, campid);
		
		List<UserPack> userlist = ("all".equals(campid) ? aggByDate(daycode) : aggByCampaign(campid));		
		
		System.out.printf("\nFound %d unique combination keys", userlist.size());
		
		//generateHistogramFile(datalines);
		
		String vizType = optMap.get("viztype");
		
		if("imps_per_user".equals(vizType)) {
			generateImpsPerUserHist(userlist);
			Util.pf("\nFinished generating impressions per user histogram.");
		} else if ("time2convert".equals(vizType)) {
			// TODO: rename this something more informative
			time2convertHist(userlist);
			Util.pf("\nFinished generating time to convert histogram.");
		} else {
			throw new RuntimeException("Unknown viztype : " + vizType);
		
		}
		
		return 0;
	}
	
	// Check to see if track file is present in local dir.
	// If so, return track data directly.
	// If not, make Hadoop file call and download it.
	/*
	LinkedList<String> getTrackSlice(String daycode) throws IOException
	{
		String localPath = Util.sprintf("%s/slice_%s.txt", TRK_SLICE_CACHE_DIR, daycode);
		
		if(!(new File(localPath)).exists())
		{
			Util.pf("\nCache file not found, reading from HDFS");
			
			FileSystem fsys = FileSystem.get(getConf());
			String convPath = Util.sprintf("/data/analytics/TRK_SLICE/slice_%s.txt", daycode);
			
			List<String> hadLines = HadoopUtil.readFileLinesE(fsys, convPath);
			FileUtils.writeFileLines(hadLines, localPath);
		}
		
		LinkedList<String> datalines = new LinkedList<String>(FileUtils.readFileLinesE(localPath));
		return datalines;
	}
	*/

	List<UserPack> aggByCampaign(String campId) throws Exception
	{
		// TODO: sequester all the SQL code somewhere else
		String campSql = "SELECT * FROM trk_lines WHERE camp_id = ? ORDER BY wtp_id,imp_ts";
		try {
			Connection conn = DbConnect.getConnection();
			
			PreparedStatement pstmt = conn.prepareStatement(campSql);
			
			pstmt.setInt(1, Integer.valueOf(campId));
			
			ResultSet rset = pstmt.executeQuery();
			
			return getUpackList(rset);
			
		} catch (SQLException sqlex) {
			
			sqlex.printStackTrace();
			return null;
		}	
	}
	
	List<UserPack> aggByDate(String daycode) throws Exception
	{

		long startAm; 
		{
			Calendar approx = Util.dayCode2Cal(daycode);
			approx.set(Calendar.HOUR_OF_DAY, 0);
			approx.set(Calendar.MINUTE, 0);
			approx.set(Calendar.SECOND, 0);
			startAm = approx.getTimeInMillis();
		}
		
		// TODO: sequester all the SQL code somewhere else
		String dateSql = "SELECT * FROM trk_lines WHERE ? < conv_ts AND conv_ts < ? ORDER BY wtp_id,imp_ts";
		
		try {
			Connection conn = DbConnect.getConnection();
			PreparedStatement pstmt = conn.prepareStatement(dateSql);	
			
			pstmt.setLong(1, startAm);
			pstmt.setLong(2, startAm+Util.DAY_MILLI);
			
			ResultSet rset = pstmt.executeQuery();
			return getUpackList(rset);
			
		} catch (SQLException sqlex) {
			
			sqlex.printStackTrace();
			return null;
		}	
	}	
	

	void generateImpsPerUserHist(List<UserPack> userlist) throws IOException
	{
		List<Double> impcountlist = Util.vector();
		
		for(UserPack upack : userlist)
		{
			impcountlist.add((double) upack.getImpCount());
		}
		
		HistogramWrapper hwrap = new HistogramWrapper();
		hwrap.setBinData(impsPerUserBin());
		hwrap.title = "Impressions Per User";
		hwrap.datalist = impcountlist;
		
		Util.pf("\nGot here, %d bin labels, %d data points", 
				hwrap.binLabels.size(), hwrap.datalist.size());
		
		hwrap.writeEps(optMap.get("outputeps"));
	}
	
	// TODO: think about how these things are really supposed to work.
	void time2convertHist(List<UserPack> userlist) throws IOException
	{
		List<Double> deltas = Util.vector();
		
		for(UserPack upack : userlist)
		{
			deltas.add((double) upack.timeDelta());	
		}
		
		HistogramWrapper hwrap = new HistogramWrapper();
		hwrap.setBinData(time2convertBin());
		hwrap.title = "Time To Conversion";
		hwrap.datalist = deltas;
		
		hwrap.writeEps(optMap.get("outputeps"));
	}
		
	static SortedMap<Double, String> impsPerUserBin()
	{
		
		//2)	Impressions per user, can we show 1, 2, 3, up to 10, then 11-15, 16-20, 21-30, 31-40, 41-50, and >50?

		int[] binEdges = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 30, 40, 50 };
		
		//int[] binEdges = new int[] { 4, 8, 12, 16, 20, 30, 40, 50, 60, 80, 100, 
		//				120, 150, 180, 200, 300, 400, 500, 600, 1000 };
						
		return labelBinMap(binEdges);		
	}

	static SortedMap<Double, String> time2convertBin()
	{
		SortedMap<Double, String> binData = Util.treemap();

		double hm = 1000*60*60;
		double dm = 24*hm;
		// 1)	For time to conversion, can we break the 2hr< into  30<mins, 30min-1hour, 1-2hour?		binData.put(2*hm, "< 2 hr");
		binData.put(hm/2, "< 30m");
		binData.put(hm, "30m-1hr");
		binData.put(2*hm, "1-2 hr");
		
		binData.put(4*hm, "2-4 hr");
		binData.put(6*hm, "4-6 hr");
		binData.put(8*hm, "6-8 hr");
		
		binData.put(16*hm, "8-16 hr");
		binData.put(dm, "16-24 hr");
		binData.put(2*dm, "1-2 day");
		binData.put(4*dm, "2-4 day");
		binData.put(6*dm, "4-6 day");
		binData.put(8*dm, "6-8 day");
		binData.put(10*dm, "8-10 day");
		binData.put(12*dm, "10-12 day");
		binData.put(14*dm, "12-14 day");
		binData.put(Double.MAX_VALUE, "> 14 day");
		
		return binData;
	}	
	
	/*
	1)	For time to conversion, can we break the 2hr< into  30<mins, 30min-1hour, 1-2hour?
	*/
	
	
	static SortedMap<Double, String> labelBinMap(int[] binedges)
	{
		int lastbin = 0;
		SortedMap<Double, String> binmap = Util.treemap();
		
		for(int be : binedges)
		{
			String blabel; 
			
			if(lastbin+1 == be) {
				blabel = "" + be;	
			} else {
				blabel = (lastbin < 100 ? Util.sprintf("%d-%d", (lastbin+1), be) : Util.sprintf("-%d", be));
			}
			
			binmap.put(be+.5, blabel);
			lastbin = be;
		}
		
		binmap.put(Double.MAX_VALUE, Util.sprintf("> %d", (lastbin+1)));
		
		return binmap;
	}
	
	

	


	
	public static List<UserPack> aggData(LinkedList<String> datalines, String campId)
	{
		List<UserPack> userlist = Util.vector();
		
		Integer targCamp = "all".equals(campId) ? null : Integer.valueOf(campId);
		
		for(String ck = peekCombKey(datalines); ck != null; ck = peekCombKey(datalines))
		{
			UserPack upack = new UserPack(ck, datalines);
			
			if(targCamp == null || upack.getCampId() == targCamp)
				{ userlist.add(upack); }
		}
		
		return userlist;
	}
	
	static String peekCombKey(LinkedList<String> datalines)
	{
		if(datalines.isEmpty())
			{ return null; }
		
		return datalines.peek().split("\t")[0];
	}
	
	// Convert a result set into user packs.
	static List<UserPack> getUpackList(ResultSet rset) throws SQLException
	{
		Map<String, UserPack> resMap = Util.treemap();
		
		while(rset.next())
		{
			String wtpid = rset.getString(1);
			int campid = rset.getInt(2);
			Calendar cnvts = Util.fromMilli(rset.getLong(3));
			Calendar impts = Util.fromMilli(rset.getLong(4));
			int lineId = rset.getInt(5);
			
			String combKey = Util.sprintf("%s___%d", wtpid, campid);
			
			if(!resMap.containsKey(combKey))
			{
				UserPack upack = new UserPack(combKey, cnvts);
				resMap.put(combKey, upack);
			}
			
			resMap.get(combKey).impLineMap.put(impts, lineId);
		}
		
		return new Vector<UserPack>(resMap.values());
	}
	
	public static class UserPack
	{
		public String combKey;
		
		public Calendar cnvTs;

		public SortedMap<Calendar, Integer> impLineMap = Util.treemap();		
		
		public UserPack(String ck, Calendar cts)
		{
			combKey = ck;
			cnvTs = cts;
		}
		
		public UserPack(String ck, LinkedList<String> datalines)
		{
			combKey = ck;
			
			while(combKey.equals(peekCombKey(datalines)))
			{
				String s = datalines.poll();
				
				String[] toks = s.split("\t");
				
				//Util.pf("\nData line is %s", s);
				
				String newCombKey = toks[0];
				Calendar newCnvTs = Util.longDayCode2Cal(toks[1].split("=")[1]);
				Calendar impCnvTs = Util.longDayCode2Cal(toks[2]);
				int newLineId = Integer.valueOf(toks[3]);				
				
				if(cnvTs == null)
				{ 
					cnvTs = newCnvTs;
				}
				
				Util.massert(combKey.equals(newCombKey), "Current key %s found %s", combKey, newCombKey);
				Util.massert(newCnvTs.getTimeInMillis() == cnvTs.getTimeInMillis(), "Current ts is %s found %s", cnvTs, newCnvTs);
				
				impLineMap.put(impCnvTs, newLineId);
			}
		}
		
		public int getImpCount()
		{
			return impLineMap.size();
		}
		
		public int getCampId()
		{
			//System.out.printf("\nComb key is %s", combKey);
			String[] toks = combKey.split("___");
			return Integer.valueOf(toks[1]);
		}
		
		public String getWtpId()
		{
			return combKey.split("___")[0];
		}
		
		long timeDelta()
		{			
			long cnvTime = cnvTs.getTimeInMillis();
			
			for(Calendar cal = impLineMap.lastKey(); !impLineMap.headMap(cal).isEmpty(); cal = impLineMap.headMap(cal).lastKey())
			{
				long tdelta = cnvTime - cal.getTimeInMillis();

				if(tdelta > 0)
				{ 
					return tdelta; 
				}
			}

			// It's not clear if this is really possible, but just in case.
			return -1;
		}
	}
	
	public static class HistogramWrapper
	{
		List<Double> binEdges;
		List<String> binLabels;
		
		List<Double> datalist;
		
		String title = "Histogram";
		
		public static String TEMPLATE_PATH = "/mnt/burfoot/epstest/plottext_temp.eps";
		
		
		public void setBinData(SortedMap<Double, String> binData)
		{
			binEdges = new Vector<Double>(binData.keySet());
			binLabels = new Vector<String>(binData.values());
		}
		
		public void writeEps(String targPath) throws IOException
		{			
			int[] bincounts = Util.histogramCounts(datalist, binEdges);
			
			List<String> templateLines = FileUtils.readFileLinesE(TEMPLATE_PATH);
			
			PrintWriter pw = new PrintWriter(targPath);
			
			for(String oneline : templateLines)
			{				
				if(oneline.indexOf("xxxHISTOGRAM_DATAxxx") > -1)
				{
					
					// example 
					// [	1	85	0	(18-29)]
					// [	2	69	0	(30-44)]
					
					for(int i = 0; i < bincounts.length; i++)
					{
						pw.printf("\n\t[%d %d 0 (%s)]", (i+1), bincounts[i], binLabels.get(i));
					}
				} else if(oneline.indexOf("xxxGRAPH_TITLExxx") > -1) {
					
					
					oneline = oneline.replace("xxxGRAPH_TITLExxx", title);
					pw.printf("\n%s", oneline);
					
				} else {
					pw.printf("\n%s", oneline);
				}
			}
			
			pw.close();			
		}
		
	}
	
	// Simple Bar Graph
	// TODO: integrate this with histogram?
	public static class BarGraphWrapper
	{
		List<Double> values = Util.vector();
		List<String> labels = Util.vector();
			
		String title = "Bar Graph";
		
		public static String TEMPLATE_PATH = "/mnt/burfoot/epstest/plottext_temp.eps";
		
		public void writeEps(String targPath) throws IOException
		{						
			List<String> templateLines = FileUtils.readFileLinesE(TEMPLATE_PATH);
			
			PrintWriter pw = new PrintWriter(targPath);
			
			for(String oneline : templateLines)
			{				
				if(oneline.indexOf("xxxHISTOGRAM_DATAxxx") > -1)
				{
					
					// example 
					// [	1	85	0	(18-29)]
					// [	2	69	0	(30-44)]
					
					for(int i = 0; i < labels.size(); i++)
					{
						pw.printf("\n\t[%d %.03f 0 (%s)]", (i+1), values.get(i), labels.get(i));
					}
				} else if(oneline.indexOf("xxxGRAPH_TITLExxx") > -1) {
					
					
					oneline = oneline.replace("xxxGRAPH_TITLExxx", title);
					pw.printf("\n%s", oneline);
					
				} else {
					pw.printf("\n%s", oneline);
				}
			}
			
			pw.close();			
		}
		
	}	
	
	
}

