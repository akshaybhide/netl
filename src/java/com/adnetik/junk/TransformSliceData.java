
package com.adnetik.analytics;

import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;


import java.util.concurrent.ArrayBlockingQueue;


// read the day's worth of sliced transform data, process it in some
// kind of intelligent way.
public class TransformSliceData extends Configured implements Tool
{	
	ArrayBlockingQueue<Dline> dlQueue = new ArrayBlockingQueue<Dline>(10000);
	Scanner lineScan;
	
	SortedSet<Long> time2conv = Util.treeset();
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new TransformSliceData(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// FileSystem	
		FileSystem fsys = FileSystem.get(getConf());
		
		Path slicePath = HadoopUtil.getSlicePath(null);
		
		lineScan = new Scanner(fsys.open(slicePath));
		
		refQ();
		//System.out.printf("\nSize is %d", dlQueue.size());

		
		runScan();
		
		//printTimeHist();
		
		lineScan.close();
		
		return 0;
	}
	
	void runScan()
	{
		Set<String> combSet = Util.treeset();
		
		while(nextCombKey() != null)
		{
			List<Dline> pack = nextImpPack();	
			
			Util.massert(!combSet.contains(pack.get(0).combKey));
			combSet.add(pack.get(0).combKey);
				
			for(String jsline : getJsData(pack))
			{
				Util.pf("%s\n", jsline);	
				
			}
		}
	}
	
	void printTimeHist()
	{
		/*
		SortedMap<Long, String> timebins = Util.treemap();

		timebins.put(10*60*1000L, "<10 minutes");
		timebins.put(20*60*1000L, "10-20 min");
		timebins.put(30*60*1000L, "20-30 min");
		timebins.put(60*60*1000L, "30min-1hour");
		
		for(int h = 2; h < 48; h++)
		{
			timebins.put(h*60*60*1000L, Util.sprintf("%d hours", h));
		}

		timebins.put(Long.MAX_VALUE, "> 48 hours");
			
		for(int i = 0; i < 5; i++)
		{
			time2conv.add(15*60*1000L+i); // fifteen minutes
			
			time2conv.add(70*60*1000L+i); // fifteen minutes
		}
		
		
		for(Long bound : timebins.keySet())
		{
			Util.pf("\nBin=%d, name=%s", bound, timebins.get(bound));	
			
		}
		
		Map<
		
		for(Long delta : time2conv)
		{
		
		}
		*/
	}
	
	public String nextCombKey()
	{
		if(dlQueue.isEmpty())
			{ return null; }
		
		return dlQueue.peek().combKey;	
	}
	
	public List<Dline> nextImpPack()
	{
		List<Dline> pack = Util.vector();
		
		String nkey = nextCombKey();
		
		while(nkey.equals(nextCombKey()))
		{
			pack.add(dlQueue.poll());
		}
		
		refQ();
		
		
		return pack;
	}
	
	void refQ()
	{
		while(dlQueue.remainingCapacity() > 0 && lineScan.hasNextLine())
		{
			Dline dl = new Dline(lineScan.nextLine());	
			dlQueue.add(dl);
		}
		
		//System.out.printf("\nSize is %d, %b, nextid=%s", dlQueue.size(), dlQueue.isEmpty(), nextCombKey());
	}
	
	static List<String> getJsData(List<Dline> pack)
	{
		List<String> jslines = Util.vector();
		
		{
			Dline first = pack.get(0);
			String convline = Util.sprintf("addConvLine('%s', %d, %d);",
				first.wtpId, first.campId, first.cnvTs.getTimeInMillis());	
			
			jslines.add(convline);
		}
		
		for(Dline oned : pack)
		{	
			String onedline = Util.sprintf("addImp(%d, %d);", oned.impTs.getTimeInMillis(), oned.lineId);
			jslines.add(onedline);	
		}
		
		return jslines;
	}
	
	static long timeToConvert(List<Dline> pack)
	{
		long convMilli = pack.get(0).cnvTs.getTimeInMillis();
		long minDelta = Long.MAX_VALUE;
		
		for(Dline d : pack)
		{
			// Skip if the impression happened after the conversion			
			if(d.impTs.after(d.cnvTs))
				{ continue; } 
			
			long cnv = d.cnvTs.getTimeInMillis();
			long imp = d.impTs.getTimeInMillis();
			
			Util.massert(convMilli == cnv);
	
			long delta = cnv - imp;
			minDelta = (delta < minDelta ? delta : minDelta);
		}
		
		return minDelta;		
	}
	
	private static class Dline 
	{
		String combKey;
		String wtpId;
		int campId;
		Calendar cnvTs;
		Calendar impTs;
		
		int lineId;
		
		public Dline(String line)
		{
			String[] toks = line.split("\t");
			
			Util.massert(toks.length == 4);
			
			combKey = toks[0];
			
			{
				String[] cktoks = combKey.split("___");
				wtpId = cktoks[0];
				campId = Integer.valueOf(cktoks[1]);
			}
			
			//Util.pf(line + "\n");
			
			impTs = Util.longDayCode2Cal(toks[2]);	
			
			{
				// example conv_ts=2011-09-21 00:05:59
				String[] a = toks[1].split("=");	
				cnvTs = Util.longDayCode2Cal(a[1]);
			}
			
			lineId = Integer.valueOf(toks[3]);
			
			//Util.pf("\nIMP=%s, CNV=%s", 
				//TimeUtil.cal2DayCode(impTs), TimeUtil.cal2DayCode(cnvTs));
		}
	}
}
