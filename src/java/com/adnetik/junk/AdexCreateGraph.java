
package com.adnetik.analytics;

import java.io.IOException;
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
import com.adnetik.analytics.CountMapWrapper.LineGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.HistogramWrapper;


public class AdexCreateGraph extends Configured implements Tool
{
	private static final String TARG_CODE = "adexvgap_gt1";
	public static final String DATA_PATH = Util.sprintf("/mnt/burfoot/adhoc/%s/part-00000", TARG_CODE);
	public static final String OUTPUT_DIR = Util.sprintf("/var/www/vizdemo/%s/", TARG_CODE);
	
	private static final String TEMP_EPS = "scrap.eps";
	
	
	protected Map<String, String> optArgs = Util.treemap();	
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new AdexCreateGraph(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		FileSystem fsys = FileSystem.get(getConf());
		
		List<String> flines = HadoopUtil.readFileLinesE(fsys, DATA_PATH);
		
		Map<String, SortedMap<Double, Double>> dmap = readDataMap(flines);
		
		
		for(String adex : dmap.keySet())
		{
			HistogramWrapper hwrap = new HistogramWrapper();
			hwrap.setBinData(binDataMap());
			hwrap.datalist = cmap2datalist(dmap.get(adex));
			hwrap.title = Util.sprintf("Value Gap for %s", adex);
			hwrap.writeEps(TEMP_EPS);
		
			
			/*
			LineGraphWrapper lgw = new LineGraphWrapper();
			lgw.dataMap = dmap.get(adex);
			
			lgw.writeEps(Util.sprintf("epsdata/%s.eps", adex));
			Util.pf("\nWrote file %s", adex);
			*/
			
			String syscall = Util.sprintf("convert %s %s/%s_hist.png",
				TEMP_EPS, OUTPUT_DIR, adex);
			
			Util.syscall(syscall);
			Util.pf("\nFinished graph for adex=%s", adex);
		}
		
		return 0;
	}
	
	List<Double> cmap2datalist(SortedMap<Double, Double> cmap)
	{
		// This is a dumb, dumb way to do things
		List<Double> dlist = Util.vector();
		
		for(Double vgap : cmap.keySet())
		{
			double count = cmap.get(vgap);
			
			for(int i = 0; i < (count-.5); i++)
			{
				dlist.add(vgap);	
			}
		}
		
		return dlist;
	}
	
	Map<String, SortedMap<Double, Double>> readDataMap(List<String> flines)
	{
		Map<String, SortedMap<Double, Double>> dmap = Util.treemap();
		
		for(String fl : flines)
		{
			String[] toks = fl.split("\t");
			String[] adex_vgap = toks[0].split(Util.DUMB_SEP);
			
			int count = Integer.valueOf(toks[1]);
			double vgap = Double.valueOf(adex_vgap[1]);
			String adex = adex_vgap[0];
			
			if(!dmap.containsKey(adex))
			{
				dmap.put(adex, new TreeMap<Double, Double>());
			}
			
			dmap.get(adex).put(vgap, (double) count);
			
			//Util.pf("\nex=%s, vgap=%.02f, count=%d", adex, vgap, count);
		}
		
		return dmap;
	}
	
	
	SortedMap<Double, String> binDataMap()
	{
		SortedMap<Double, String> bdm = Util.treemap();
		
		double[] binedge = new double[] { .02, .05, .10, .20, .40, .8, 1.2, 1.5, 2.0, 
		2.5, 3.0, 3.5, 4.0, 4.5, 5, 5.5, 6, 7, 8, 9, 10};
		
		bdm.put(.02,  "< .02");
		
		for(int i = 1; i < binedge.length; i++)
		{
			String label = Util.sprintf("-%.02f", binedge[i]);
			bdm.put(binedge[i], label);
		}
		
		bdm.put(100000000.0, "> 10");		
		return bdm;
	}
}

