
package com.adnetik.analytics;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

// TODO: merge these things into one file
import com.adnetik.analytics.CountMapWrapper.LineGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.BarGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.HistogramWrapper;

public  class UserReachGraphs extends Configured implements Tool
{
	String daycode;
	String outputDir;
	
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new UserReachGraphs(), args);
		//urs.generateGraphs(args);
	}	
	
	public int run(String[] args) throws Exception
	{
		// TODO: check for a valid daycode
		daycode = args[0];
		outputDir = Util.sprintf("/var/www/vizdemo/userreach/day%s", daycode);
		(new File(outputDir)).mkdirs();
		
		generateGraphs(FileSystem.get(getConf()));
		return 1;
	}
	
	void generateGraphs(FileSystem fsys) throws IOException
	{		
		String outputPath = UserReachScan.getOutputPath(daycode) + ".txt";
		Util.pf("\nOutput path is %s", outputPath);
		
		//Util.pf("\nFound filesystem");
		Scanner sc = HadoopUtil.hdfsFileScanner(fsys, outputPath);
		
		int curLineId = -1;
		int curCampId = -1;
		
		SortedMap<Integer, Integer> lineMetaCount = Util.treemap();
		Map<String, SortedMap<Integer, Integer>> campMetaCountPack = Util.treemap();
		for(String s : new String[] { "all", "retargeting", "nonret" })
		{
			SortedMap<Integer, Integer> onemap = Util.treemap();	
			campMetaCountPack.put(s, onemap);
		}
	
		
		while(sc.hasNextLine())
		{
			String line = sc.nextLine();
			String[] key_count = line.split("\t");
			String[] stuff = key_count[0].split(Util.DUMB_SEP);
			
			Util.massertEq(stuff.length, 4);
			
			Integer campId = Integer.valueOf(stuff[0]);
			Integer lineId = Integer.valueOf(stuff[1]);
			String lineType = stuff[2];
			
			//Util.pf("\nLine type is %s", lineType);
			
			String wtpId = stuff[3];
			int count = Integer.valueOf(key_count[1]);
			
			if(wtpId.length() < 10)
			{
				//Util.pf("\nSkipping user id %s", lineWtp[1]);	
				continue;
			}
			
			if(lineId != curLineId && lineMetaCount.size() > 100)
			{
				writeImpCount(lineMetaCount, "line", lineId);
				writeGraphData(lineMetaCount, "line", lineId);

				lineMetaCount.clear();
				curLineId = lineId;
			}
			
			if(campId != curCampId)
			{
				for(String k : campMetaCountPack.keySet())
				{
					String pref = "camp_" + k;
					//Util.pf("\nCount for pack %s is %d", k, campMetaCountPack.get(k).size());
					if(campMetaCountPack.get(k).size() > 5)
					{
						writeImpCount(campMetaCountPack.get(k), pref, campId);
						writeGraphData(campMetaCountPack.get(k), pref, campId);					
					}
					campMetaCountPack.get(k).clear();
				}
				
				curCampId = campId;
			}
			
			Util.incHitMap(lineMetaCount, count);
			Util.incHitMap(campMetaCountPack.get("all"), count);
			Util.incHitMap(campMetaCountPack.get("retargeting".equals(lineType) ? lineType : "nonret"), count);
		}
		
		sc.close();
	}
	
	void writeImpCount(SortedMap<Integer, Integer> metacount, String pref, int targId) throws IOException
	{
		HistogramWrapper hw = new HistogramWrapper();
		hw.datalist = Util.vector();
		
		for(int mcount : metacount.keySet())
		{
			for(int uc = 0; uc < metacount.get(mcount); uc++)
				{ hw.datalist.add((double) mcount); }
		}
		
		SortedMap<Double, String> impsBin = EpsWrapperTool.impsPerUserBin();
		hw.setBinData(impsBin);		
		
		hw.title = Util.sprintf("Users with given Impression Total - Total Unique Users=%d", 
			hw.datalist.size());
		hw.writeEps(Util.SCRAP_EPS);
		
		String pngout = Util.sprintf("%s/impcount_%s_%d.png", outputDir, pref, targId);
		doConvert(pngout);
	}
	
	private void doConvert(String pngout) throws IOException
	{
		List<String> reslist = Util.vector();
		List<String> errlist = Util.vector();
		
		String syscall = Util.sprintf("convert %s %s", Util.SCRAP_EPS, pngout);
		Util.syscall(syscall, reslist, errlist);
		
		if(!errlist.isEmpty())
		{
			for(String err : errlist)
			{
				Util.pf("\n\terr: %s", err);	
			}
			System.exit(1);
		}
	}
	
	void writeGraphData(SortedMap<Integer, Integer> metacount, String pref, int targId) throws IOException
	{
		Util.pf("\nWriting for pref %s", pref);
		
		BarGraphWrapper bgw = new BarGraphWrapper();
		
		double[] percMax = new double[] { .005, .01, .05, .1, .15, .2, .3, .4, .5, .6 };
		double[] percAbv = percentAbove(metacount, percMax);
		
		bgw.title = "Distribution of Ads to Users";
		
		for(int i = 0; i < percMax.length; i++)
		{
			bgw.labels.add(Util.sprintf("Top %.01f%s", percMax[i]*100, "%"));
			bgw.values.add(percAbv[i]);
		}
		
		bgw.writeEps(Util.SCRAP_EPS);
		
		String pngout = Util.sprintf("%s/impdist_%s_%d.png", outputDir, pref, targId);
		doConvert(pngout);
	}
	
	double[] percentAbove(SortedMap<Integer, Integer> metacount, double[] topx)
	{
		double[] pa = new double[topx.length];
		List<Integer> keylist = new Vector<Integer>(metacount.keySet());
		Collections.sort(keylist, Collections.reverseOrder());
		
		Util.pf("\nMetacount size is %d", metacount.size());
		double usertotal = 0;
		double imptotal = 0;
		for(Integer impcount : keylist)
		{
			usertotal += metacount.get(impcount);
			imptotal += impcount * metacount.get(impcount);
		}
		
		Util.pf("\nUser total is %.03f, imp total is %.03f", usertotal, imptotal);
		double curtotal = 0;
		
		// start with large impcounts
		for(Integer impcount : keylist)
		{
			// for each user with 
			for(int uc = 0; uc < metacount.get(impcount); uc++)
			{
				for(int i = 0; i < topx.length; i++)
				{
					if(curtotal / usertotal < topx[i])
					{ 
						pa[i] += impcount / imptotal;
					}
				}
				curtotal++;
			}
		}
		
		return pa;
	}
}
