/**
 * 
 */
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.shared.BidLogEntry.*;


public class WtpIdTest
{

	Map<WtpId, Set<Integer>> pixLookupMap = Util.treemap();
	
	List<String> libLines;
	
	public static void main(String[] args) throws Exception
	{
		
		Util.pf("WtpIdTest");
		//showSomeInfo();
		
		WtpIdTest wit = new WtpIdTest();
		wit.readLibLines(200000);
		//wit.testCompareTo();
		wit.initLookupMap();
		
		//wit.showMapKeys();
		
	}
	
	void showMapKeys()
	{
		for(WtpId id : pixLookupMap.keySet())
		{
			Util.pf("%s\n", id.toString());
		}
	}
	
	void testCompareTo()
	{
		List<WtpId> wtplist = Util.vector();
		
		for(String s : libLines)
		{ 
			String[] wtp_pix = s.split("\t");
			
			WtpId shortform = null;
			
			try {  shortform = new WtpId(wtp_pix[0]); }
			catch (Exception ex) { }
			
			if(shortform == null)
				{ continue; }
			
			wtplist.add(shortform);
		}			
		
		Random jrand = new Random();
		
		for(int i = 0; i < 1000000; i++)
		{
			WtpId a = wtplist.get(jrand.nextInt(wtplist.size()));
			WtpId b = wtplist.get(jrand.nextInt(wtplist.size()));
			
			int compFAST = a.compareTo(b);
			int compSLOW = a.compareToSub(b);
			
			compSLOW = (compSLOW < 0 ? -1 : compSLOW);
			compSLOW = (compSLOW > 0 ? +1 : compSLOW);
			
			//Util.pf("\nfast=%d, slow=%d", compFAST, compSLOW);
			if(compSLOW != compFAST)
			{
				Util.pf("\nFailed for wtps \n\ta=%s\n\tb=%s", a, b);
				
				Util.pf("Slow=%d, fast=%d", compSLOW, compFAST);
				Util.massert(compFAST == compSLOW);
				
			}
		}
	}
	
	void readLibLines(int max) throws Exception
	{
		libLines = new Vector<String>(max);
		
		double startup = System.currentTimeMillis();
		
		//BufferedReader bread = HadoopUtil.hdfsBufReader(FileSystem.get(job), localFiles[0]);
		Scanner sc = new Scanner(new File("lookuptest.txt"));
		
		for(int i = 0; i < max && sc.hasNextLine(); i++)
		{
			libLines.add(sc.nextLine().trim());
			
			//Util.pf("\nRead %d lines", libLines.size());
		}
		
		double endtime = System.currentTimeMillis();
		
		Util.pf("\nRead %d lines, took %.03f secs", libLines.size(), (endtime - startup)/1000);
		
	}
	
	
	void initLookupMap() throws IOException
	{
		pixLookupMap = Util.treemap();
		
		double startup = System.currentTimeMillis();
	
		for(String line : libLines)
		{
			String[] wtp_pix = line.split("\t");
			
			WtpId shortform = null;
			
			try {  shortform = new WtpId(wtp_pix[0]); }
			catch (Exception ex) { }
			
			if(shortform == null)
				{ continue; }
			
			Integer pixid = Integer.valueOf(wtp_pix[1]);
			
			if(!pixLookupMap.containsKey(shortform))
				{ pixLookupMap.put(shortform, new TreeSet<Integer>()); }
			
			pixLookupMap.get(shortform).add(pixid);
			
			//Util.pf("\nPix lookup map size is %d", pixLookupMap.size());
		}
		
		double endtime = System.currentTimeMillis();

		Util.pf("\nPIx count is %d, took %.03f secs", pixLookupMap.size(), (endtime - startup)/1000);
				
	}	
	
	
}
