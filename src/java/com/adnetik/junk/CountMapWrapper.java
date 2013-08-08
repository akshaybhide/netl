
package com.adnetik.analytics;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

public class CountMapWrapper
{
	Map<String, String> optArgs = Util.treemap();
	
	public static void main(String[] args) throws Exception
	{
		(new CountMapWrapper()).run(args);
		
		
	}
	
	void run(String[] args) throws Exception
	{
		{
			optArgs.put("outputeps", "/mnt/burfoot/epstest/linetest.eps");
			optArgs.put("campid", "476");
			Util.putClArgs(args, optArgs);
		}		
		
		Util.pf("\nOptional arguments: ");
		for(String keyarg : optArgs.keySet())
		{
			Util.pf("\n\t%s ==> %s", keyarg, optArgs.get(keyarg));			
		}
		
		
		SortedMap<Double, Double> countMap = getCountMap(Integer.valueOf(optArgs.get("campid")));
		rankedOrderGraph(countMap, optArgs.get("outputeps"));
	}
	
	SortedMap<Double, Double> getCountMap(int campId)
	{	
		String vcountSql = "SELECT wtp_id, count(*) AS ccount FROM trk_lines WHERE camp_id = ? GROUP BY wtp_id ORDER BY ccount";
		
		List<Double> vclist = Util.vector();
		
		try {
			Connection conn = DbConnect.getConnection();
			PreparedStatement pstmt = conn.prepareStatement(vcountSql);	
			
			pstmt.setLong(1, campId);
			
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				vclist.add(rset.getDouble(2));	
				
			}
			
		} catch (SQLException sqlex) {
			
			sqlex.printStackTrace();
			return null;
		}			
		
		Collections.reverse(vclist);
		SortedMap<Double, Double> cmap = Util.treemap();
		
		for(int i = 0; i < vclist.size(); i++)
		{
			cmap.put((double) i+1, vclist.get(i));
		}
		
		return cmap;		
		
	}
	
	void showCampIds()
	{	
		String vcountSql = "select camp_id, count(*) from trk_lines group by camp_id";
				
		try {
			Connection conn = DbConnect.getConnection();
			PreparedStatement pstmt = conn.prepareStatement(vcountSql);	
					
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				int campId = rset.getInt(1);
				Util.pf("\n\tcampIdList.push('%d');", campId);
			}
			
		} catch (SQLException sqlex) {
			
			sqlex.printStackTrace();
		}			
	}	
	
	
	void rankedOrderGraph(SortedMap<Double, Double> dataMap, String outputPath) throws IOException
	{
		Util.pf("Calling ranked order graph");
		Util.pf("\nFirst x/y pair is %.03f, %.03f", dataMap.firstKey(), dataMap.get(dataMap.firstKey()));
		
		
		LineGraphWrapper lgw = new LineGraphWrapper();
		lgw.dataMap = dataMap;
		
		lgw.title = "Number of Impressions Per User";
		lgw.leftLab = "Number of Impressions";
		lgw.bottomLab = "User Index";
		
		lgw.writeEps(outputPath);
		
		
	}
	
	public static class LineGraphWrapper
	{
		SortedMap<Double, Double> dataMap;
		
		double dataMax = -1;
	
		public String bottomLab = "Bottom Label";
		public String leftLab = "Left Label";
		
		public String title = "LineGraph";
		
		public static String TEMPLATE_PATH = "/mnt/burfoot/epstest/LineGraphTemplate.eps";
		
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
					
					for(Double x : dataMap.keySet())
					{
						double y = dataMap.get(x);
						dataMax = y > dataMax ? y : dataMax;
						pw.printf("\n\t[%.03f %.03f]", x, y);
					}


				} else if(oneline.indexOf("xxxGRAPH_WIDTHxxx") > -1) {
					oneline = oneline.replace("xxxGRAPH_WIDTHxxx", "" + dataMap.lastKey());
					pw.printf("\n%s", oneline);
				} else if(oneline.indexOf("xxxGRAPH_HEIGHTxxx") > -1) {
					oneline = oneline.replace("xxxGRAPH_HEIGHTxxx", "" + dataMax);
					pw.printf("\n%s", oneline);
				} else if(oneline.indexOf("xxxGRAPH_LEFTxxx") > -1) {
					oneline = oneline.replace("xxxGRAPH_LEFTxxx", leftLab);
					pw.printf("\n%s", oneline);	
				} else if(oneline.indexOf("xxxGRAPH_BOTTOMxxx") > -1) {
					oneline = oneline.replace("xxxGRAPH_BOTTOMxxx", bottomLab);
					pw.printf("\n%s", oneline);					
				} else if(oneline.indexOf("xxxGRAPH_TITLExxx") > -1) {
					oneline = oneline.replace("xxxGRAPH_TITLExxx", title);
					pw.printf("\n%s", oneline);					
				} else {
					pw.printf("\n%s", oneline);
				}
			}
			
			pw.close();			
		}
		
		public void setDataFromList(Vector<Double> dlist)
		{
			dataMap = Util.treemap();
			Collections.sort(dlist, Collections.reverseOrder());
			
			int c = 0;
			for(Double d : dlist)
			{ 
				if(dataMap.size() > 500)
					{ break; }
				
				c += 1;
				dataMap.put((double) c, d); 
				Util.pf("\nPutting value %.03f, %.03f", (double) c, d);
			}
		}
		
		public void setDataFromCountMap(Map<? extends Number, Integer> countmap)
		{			
			Vector<Double> dlist = Util.vector();
			
			for(Number c : countmap.keySet())
			{
				int num = countmap.get(c);

				for(int i = 0; i < num; i++)
					{ dlist.add(c.doubleValue()); }
			}
			
			setDataFromList(dlist);
		}
	}		
}
