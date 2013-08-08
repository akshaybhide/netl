
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.analytics.*;

public class SegmentLookupTest
{		
	FSDataInputStream _segStream;
	
	long _totalSize;
	
	Path _targPath;
	
	FileSystem _fSystem;
	
	int _probeCount;
	
	public static void main(String[] args) throws IOException
	{		
		SegmentLookupTest slt = new SegmentLookupTest();
		List<String> idlist = FileUtils.readFileLinesE("testids.txt");
		double startup = Util.curtime();
		
		for(int i = 10; i < 20; i++)
		{
			String oneid = idlist.get(i);
			
			// long binpos = slt.interpSearch(oneid);
			long binpos = slt.binSearch(oneid);
			
			if(binpos == -1)
				{ continue;}
			
			long intpos = slt.interpSearch(oneid);
			
			Util.pf("BINPOS=%d, INTPOS=%d\n", binpos, intpos);
			
			if(binpos == -1)
			{
				// Util.pf("Failed to find cookie %s after %d probes\n", oneid, slt._probeCount);
			} else {
			
				List<String> segdata = slt.getSegmentData(oneid, binpos);
			}
			// Util.pf("Found target ID %s after %d probes\n", oneid, slt._probeCount);
			/*
			for(String oneline : slt.getSegmentData(oneid, binpos))
			{
				// Util.pf("Result line is %s\n", oneline);			
			}
			*/
			
			if(((i+1) % 10) == 0)
			{
				Util.pf("Finished with lookup i=%d, avg time is %.03f/lookup\n",
					i, (Util.curtime()-startup)/(1000*(i+1)));
				
				break;
			}
		}	

	}
	
	public SegmentLookupTest() throws IOException
	{
		_targPath = new Path("/thirdparty/bluekai/snapshot/MASTER_LIST_2013-03-25.txt");
		_fSystem = FileSystem.get(new Configuration());
		
		_totalSize = _fSystem.getFileStatus(_targPath).getLen();
		
		_segStream = _fSystem.open(_targPath);
		
		Util.pf("Initialized SLT, file size is %d\n", _totalSize);
	}
	
	void doSeek(long targ) throws IOException
	{
		_segStream.seek(targ);
	}
	
	String nextId() throws IOException
	{
		BufferedReader bread = new BufferedReader(new InputStreamReader(_segStream));
		
		// Throwaway; we're in the middle of this line.
		bread.readLine();
		String[] toks = bread.readLine().split("\t");
		
		// bread.close();
		
		return toks[0];
	}
	
	List<String> getSegmentData(String idtarg, long foundpos) throws IOException
	{
		List<String> seglist = Util.vector();
		long startfrom = foundpos;
		
		while(true)
		{
			String probe = probeId(startfrom);
			if(!probe.equals(idtarg))
			{
				// Util.pf("Found probe %s compared to ID %s, done with rewind\n", probe, idtarg);
				break; 
			}
			
			// Util.pf("ID at startfrom=%d is still %s, rewinding...\n", startfrom, probe);
			startfrom -= 1000;
		}

		Util.massert(startfrom < foundpos, "Did not rewind at all");
		
		doSeek(foundpos);
		boolean found = false;
		BufferedReader bread = new BufferedReader(new InputStreamReader(_segStream));
		bread.readLine();
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String[] toks = oneline.split("\t");
			
			if(idtarg.equals(toks[0]))
			{
				seglist.add(oneline);
				found = true;
			} else {
				
				// Here we have FOUND some results, but have now passed the region corresponding to the 
				// target ID
				if(found)
					{ break; }
			}	
		}
		
		bread.close();
		
		return seglist;
	}
	
	
	String probeId(long targ) throws IOException
	{
		doSeek(targ);
		return nextId();		
	}
	
	long binSearch(String wtptarg) throws IOException
	{
		_probeCount = 0;
		return binSearchSub(wtptarg, 0, _totalSize);
	}
	
	long binSearchSub(String wtptarg, long min, long max) throws IOException
	{
		
		if(max - min < 50)
		{
			// Util.pf("Min=%d, Max=%d but targ not found, search failed\n", min, max);
			return -1;
		}
		
		long mid = (max+min)/2;
		String probe = probeId(mid);
		_probeCount++;
		
		// Util.pf("Probed position=%d, result is %s\n", mid, probe);
		
		int comp = wtptarg.compareTo(probe);
		
		if(comp < 0)
		{
			// targ is before probe	
			return binSearchSub(wtptarg, min, mid);
		} else if(comp > 0) {
			
			return binSearchSub(wtptarg, mid, max);	
		} else {
			// Util.pf("FOUND target id %s at position %d\n", wtptarg, mid);
			return mid;	
		}
		
	}
	
	long interpSearch(String trgid) throws IOException
	{
		String minid = "00000000-0000-0000-0000-000000000000";
		String maxid = minid.replaceAll("0", "F");
		
		return interpSearchSub(trgid, 0, minid, _totalSize, maxid, 0);
		
	}
	
	long interpSearchSub(String trgid, long low, String lowid, long hgh, String hghid, int depth) throws IOException
	{
		Util.massert(depth < 40, "Arrived at depth=40, bailing out");
		
		if(hgh - low < 50)
		{
			Util.pf("Min=%d, Max=%d but targ not found, search failed\n", low, hgh);
			return -1;
		}
		
		double targfrac = calcTargFrac(lowid, trgid, hghid);
		Util.pf("LOW=%s, TRG=%s, HGH=%s, frac=%.08f\n", lowid, trgid, hghid, targfrac);
		
		long prb = (long) (low + targfrac * (hgh-low));

		Util.pf("LOW=%d, HGH=%d, PROBE=%d\n", low, hgh, prb);

		String prbid = probeId(prb);
		_probeCount++;
		
		// Util.pf("Probed position=%d, result is %s\n", mid, probe);
		
		int comp = trgid.compareTo(prbid);
		
		if(comp < 0)
		{
			// targ is before probe	
			return interpSearchSub(trgid, low, lowid, prb, prbid, depth+1);
		} else if(comp > 0) {
			
			return interpSearchSub(trgid, prb, prbid, hgh, hghid, depth+1);
		} else {
			Util.pf("FOUND target id %s at position %d\n", trgid, prb);
			return prb;	
		}		
	}

	private double calcTargFrac(String low, String trg, String hgh)
	{
		double lowval = 0;
		double trgval = 0;
		double hghval = 0;
		
		for(int i = 0; i < low.length(); i++)
		{
			if(low.charAt(i) == '-')
				{ continue; }
			
			lowval *= 16;
			trgval *= 16;
			hghval *= 16;
			
			lowval += Integer.parseInt(""+low.charAt(i), 16);
			trgval += Integer.parseInt(""+ trg.charAt(i), 16);
			hghval += Integer.parseInt(""+hgh.charAt(i), 16);
			
			
			if(hghval - lowval > 32000)
			{
				break;
			}
		}
		
		return (trgval - lowval) / (hghval - lowval);
	}
}
