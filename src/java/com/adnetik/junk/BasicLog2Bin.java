/**
 * 
 */
package com.adnetik.bm_etl;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place


public class BasicLog2Bin extends Configured implements Tool 
{
	//public static final int NUM_SHARDS = 1000;
	public static final int NUM_SHARDS = 1;
	
	boolean expandDate = false;
	private SortedSet<DimCode> dimSet;
	private String dayCode;
	
	private Map<Integer, PrintWriter> writeMap = Util.treemap();
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
				
		
		dayCode = args[0];
		dayCode = ("yest".equals(dayCode) ? TimeUtil.getYesterdayCode() : dayCode);
		if(!TimeUtil.checkDayCode(dayCode))
		{ 
			Util.pf("\nBad day code : %s" , dayCode);
			System.exit(1);
		}
		
		Map<String, String> optargs = Util.getClArgMap(args);
		boolean isinternal = "true".equals(optargs.get("intern"));
		Integer maxFileCount = optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : null;
		
		//CatalogUtil.createAllCatalogs(getConf(), dayCode);	
		CatalogUtil.initSing(dayCode, (isinternal ? DbTarget.internal : DbTarget.external));

		// Set various strings related to job
		{
			dimSet = DatabaseBridge.getDimSet(AggType.ad_general, isinternal ? DbTarget.internal : DbTarget.external);
			Util.pf("\nDimension set is %s", dimSet);
		}
					
		// Open all the output writers
		initWriters();
		
		// Deal with input files
		List<String> pathlist = getPathList(dayCode, maxFileCount, true);
		Collections.shuffle(pathlist);
		double fstart = Util.curtime();
		
		for(int i = 0; i < pathlist.size(); i++)
		{
			double startup = Util.curtime();
			String onepath = pathlist.get(i);
			processFile(pathlist.get(i), i);
			double endtime = Util.curtime();
			Util.pf("\nProcessing path %d/%d, took %.03f secs, total %.03f, avg=%.03f/file", 
				i, pathlist.size(), (endtime-startup)/1000, (endtime-fstart)/1000, (endtime-fstart)/(1000*(i+1)));		
		}
		
		closeWriters();
		
		
		return 1;
	}
	
	void initWriters() throws Exception
	{
		for(int i = 0; i < NUM_SHARDS; i++)
		{
			String wpath = Util.sprintf("/mnt/data/burfoot/bm-etl/shards/shard-%s.txt", Util.padLeadingZeros(i, 4));
			PrintWriter pwrite = new PrintWriter(new File(wpath));
			writeMap.put(i, pwrite);
		}
	}
	
	void closeWriters() throws Exception
	{
		for(Integer key : writeMap.keySet())
			{ writeMap.get(key).close(); }	
	}	
	
	
	void processFile(String onepath, int pathid) throws IOException
	{	
		PathInfo relPath = new PathInfo(onepath);		
		
		BufferedReader bread = Util.getGzipReader(onepath);
		
		for(String logline = bread.readLine(); logline != null; logline = bread.readLine())
		{
			try {
				String[] values = logline.split("\t");
				
				BleStructure clone = BleStructure.buildStructure(relPath.pType, relPath.pVers, logline);
				
				DumbDimAgg dumbagg = new DumbDimAgg(clone.logentry);
				Metrics magg = clone.returnMetrics();
				Util.massert(false, "Need to refactor");
				// magg.updateCostCurrencyInfo(relPath);
				
				//writeAggInfo(dumbagg, magg);
				
			} catch (Exception iex) {
				
				// TODO: change this to a counter-increment.
				throw new RuntimeException(iex);	
				
			}
		}
	}
	
	void writeAggInfo(DumbDimAgg ddagg, Metrics magg)
	{
		String key = ddagg.computeKey(dimSet, expandDate);
		int hcode = key.hashCode();
		hcode = (hcode < 0 ? -hcode : hcode);
		hcode %= NUM_SHARDS;
		
		// Util.pf("\nHCode is %d for key=%s", hcode, key);
		
		StringBuffer sb = new StringBuffer();
		sb.append(key);
		sb.append("\t");
		sb.append(magg.toString("="));
		sb.append("\n");
		writeMap.get(hcode).write(sb.toString());
	}

	
	List<String> getPathList(String daycode, Integer maxcount, boolean usehdfs) throws IOException
	{
		List<String> subblist = Util.vector();
		// LogType[] touse = new LogType[] { LogType.click, LogType.conversion };
		LogType[] touse = new LogType[] { LogType.imp, LogType.click, LogType.conversion };
		// LogType[] touse = new LogType[] { LogType.bid_all, LogType.imp, LogType.click, LogType.conversion };
		// LogType[] touse = new LogType[] { LogType.imp };
		FileSystem fsys = FileSystem.get(getConf());
		
		
		for(LogType ltype : touse)
		{			
			for(ExcName exc : ExcName.values())
			{
				if(ltype == LogType.bid_all)
				{
					throw new RuntimeException("Bid logs not yet implemented");
					
				} else {
										
					List<String> exlist = Util.getNfsLogPaths(exc, ltype, daycode);
					
					if(exlist == null)
						{ continue; }
					
					for(String onepath : exlist)
					{
						subblist.add(onepath);
					}			
				}
			}		
		}
		
		List<String> pathlist = Util.vector();
		
		for(String p : subblist)
		{
			pathlist.add(p);
			
			if(maxcount != null && pathlist.size() >= maxcount)
				{ break; }
		}
		
		// Util.pf("\nPath list is %s", pathlist);
		
		return pathlist;
	}
}
