

package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.sql.*;


import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;

public abstract class ExelateLookup
{
	private static ExelateLookup _SING;
	
	public static ExelateLookup getSing()
	{
		if(_SING == null)
		{
			_SING = new ExelateLookup.HdfsLookup(true);
			//_SING = new ExelateLookup.GpLookup();
		}
		
		return _SING;
	}
	
	public static void main(String[] args) throws Exception
	{	
		int lookups = 0;
		int hits = 0;
		
		Scanner sc = new Scanner(new File("test_wtp_ids.txt"));
		
		while(sc.hasNextLine())
		{
			String exline = sc.nextLine().trim();
			
			if(exline.length() == 0)
				{ continue; }
			
			ExPack expack = getSing().lookup(exline);
			
			lookups += 1;
			hits += (expack != null ? 1 : 0);
		}
		
		sc.close();
		
		//getSing().doneReading();
		
		Util.pf("\nGot %d hits out of %d lookups", hits, lookups);
	}
	
	
	public static void main2(String[] args) throws Exception
	{
		ExelateLookup exlook = new ExelateLookup.FSystem(false);
		int lcount = 0;
		
		Scanner sc = new Scanner(new File("sort_ex_20120213.txt"));
		
		while(sc.hasNextLine())
		{
			String exline = sc.nextLine().trim();
			
			if(exline.length() == 0)
				{ continue; }
			
			exlook.feedLine(exline);	
			
			//if(lcount++ > 100000)
			//	{ break; }
			
		}
		
		sc.close();
		
		exlook.flush();
		
		Util.pf("\nTotal writes is %d\n", exlook.getWriteCount());
	}

	
	public abstract ExPack lookup(String wtpid) ;
	
	public abstract void doneReading(); 
	
	public abstract void restart();
	
	public abstract void feedLine(String exlogline) throws Exception;
	
	public abstract void flush() throws Exception;
	
	public abstract int getWriteCount(); 
	
	public static class ExPack
	{
		public Long timestamp;	
		
		public String country; 
		
		public String segments;
		
		public Map<Integer, Integer> idmap;
		
		
		public ExPack()
		{
			idmap = Util.treemap();
			
		}
		
		public ExPack(String[] data)
		{
			Util.massert(data.length == 3 || data.length == 4);
			
			timestamp = Long.valueOf(data[0]);
			country = data[1];
			
			// WTP may or may not be present.
			
			segments = data[data.length-1];
		}
		
		
		public boolean hasTopId(int id)
		{
			if(idmap == null)
			{
				idmap = Util.treemap();
				String[] toks = segments.split(",");
				
				for(String onetok : toks)
				{
					try {
						String[] top_sub = onetok.split("-");
						
						int daysin = Integer.valueOf(top_sub[0]);
						int segmentid = Integer.valueOf(top_sub[1]);
						
						idmap.put(segmentid, daysin);
					} catch (NumberFormatException nfex) {
						// no biggie	
					}
				}
			}
			
			return idmap.containsKey(id);
		}
	}
	
	public static class FSystem extends ExelateLookup
	{
		String curWtp;
		
		TreeMap<String, String[]> curBatch;
		
		String curPath;
		
		int reloads = 0;
		
		boolean readMode = true;
		
		int writeCount = 0;
		
		public FSystem(boolean read)
		{
			readMode = read;
			
			curWtp = Util.WTP_ZERO_ID;
			curPath = pathFromId(curWtp);
			loadBatch(); // do I allow loadBatch to take an argument...?
		}
		
		
		public void doneReading()  
		{
			throw new RuntimeException("not yet implemented");	
			
		}
		
		public ExPack lookup(String wtpid)
		{
			Util.massert(curWtp.compareTo(wtpid) <= 0, "Lookup called out of order");
			curWtp = wtpid;
			
			if(curPath != pathFromId(curWtp))
			{
				loadBatch();
			}
			
			String[] data = curBatch.get(wtpid);
			
			return (data == null ? null : new ExPack(data)); 
		}
		
		public void restart()
		{
			
			
		}
		
		public int getWriteCount()
		{
			return writeCount;	
			
		}
		
		// Ok we're in writing mode.
		public void feedLine(String exline) throws IOException
		{
			String[] newtoks = exline.split("\t");
			
			//Util.pf("\nLine is %s, a-tok is %s", exline, newtoks[0]);

			Util.massert(!readMode, "We are in readmode, not writemode.");
			
			try {
				Util.massert(newtoks.length == 4, "Invalid log line, number of tokens = " + newtoks.length);
				Util.massert(curWtp.compareTo(newtoks[2]) <= 0, "CurWTP %s comes after newid %s", curWtp, newtoks[2]);
			} catch (Exception ex) {
				
				// I guess we just skip the line.
				ex.printStackTrace();
				return;
			}
			
			curWtp = newtoks[2];

			if(!curPath.equals(pathFromId(curWtp)))
			{
				// Write the old batch. 
				writeCurBatch();
								
				// Load the new batch.
				loadBatch();
			}
			
			String[] prvtoks = curBatch.get(curWtp);
			
			// TODO: do I really need to convert to Long-format here, can't I just do String compares?
			if(prvtoks == null || Long.valueOf(prvtoks[0]) < Long.valueOf(newtoks[0]))
			{
				curBatch.put(curWtp, new String[] { newtoks[0], newtoks[1], newtoks[3] });
			} 
		}
		
		private String pathFromId(String wtpid)
		{
			String toppref = wtpid.substring(0, 2);
			String subpref = wtpid.substring(2, 4);
			
			// Util.pf("\nDirs are %s, %s", toppref, subpref);
			
			return Util.sprintf("/mnt/data/burfoot/exelate/data/%s/batch__%s__%s.txt", toppref, toppref, subpref);
		}
		
		private void loadBatch()
		{
			curBatch = Util.treemap();
			curPath = pathFromId(curWtp);
			
			try {
				if((new File(curPath)).exists())
				{
					BufferedReader bread = Util.getReader(curPath, "US-ASCII");
					
					for(String line = bread.readLine(); line != null; line = bread.readLine())
					{
						String[] toks = line.split("\t");					
						String newid = toks[2];
						curBatch.put(newid, new String[] {toks[0], toks[1], toks[3] });
					}
					
					bread.close();
				}
			} catch (IOException ioex) {
				
				ioex.printStackTrace();
				throw new RuntimeException(ioex);
			}
		}
		
		private void writeCurBatch() throws IOException
		{
			Util.pf("\nWriting %d users to path %s", curBatch.size(), curPath);			
			
			(new File(curPath)).getParentFile().mkdirs();
			
			PrintWriter pwrite = new PrintWriter(curPath);
			
			for(String wtpid : curBatch.keySet())
			{
				String[] toks = curBatch.get(wtpid);
				// Util.pf("\nFor ID=%s, segments are %s", wtpid, toks[2]);
				pwrite.printf("%s\t%s\t%s\t%s\n", toks[0], toks[1], wtpid, toks[2]);
			}
			
			pwrite.close();
			
			writeCount++;
		}
		
		public void flush() throws IOException
		{
			Util.pf("\nFlushing batch to path %s", curPath);
			writeCurBatch();	
		}
	}
	
	public static class HdfsLookup extends ExelateLookup
	{
		// purely for sanity-checking
		String lastLookup = Util.WTP_ZERO_ID;
		
		BufferedReader curReader;
		int curPartId;
		
		FileSystem fSystem;
		
		LinkedList<Pair<String, ExPack>> theQ = Util.linkedlist();
		
		public int partTotal = 20; // TODO: need to set this dynamically
		
		public static final int TARG_QSIZE = 100000;
		
		public HdfsLookup(boolean read)
		{
			Util.massert(read, "All write operations for this class done as Hadoop jobs");
			
			try {
				Configuration conf = new Configuration();
				fSystem = FileSystem.get(conf);
				
				
				curReader = HadoopUtil.hdfsBufReader(fSystem, partPath(curPartId));
				refillQ();
				
			} catch (Exception ex) {
				
				throw new RuntimeException(ex);	
			}
		}
		
		
		public ExPack lookup(String wtpid)
		{
			// Just sanity checking here
			{
				Util.massert(lastLookup.compareTo(wtpid) <= 0, "Lookup called out of order");
				lastLookup = wtpid;
			}
			
			// Util.pf("\nCur wtp is %s", wtpid);
			
			
			while(!theQ.isEmpty() && peekUserId().compareTo(wtpid) < 0)
			{
				// This is going to poll and refresh if necessary
				try { pollCheck(); }
				catch (IOException ioex) { throw new RuntimeException(ioex); }
			}
			
			// No more info in the Q, return.
			if(theQ.isEmpty())
				{ return null; }
			
			if(peekUserId().compareTo(wtpid) > 0)
			{
				// Went past the target, wtpid must not be here.
				// Util.pf("\nCould not find %s, top is now %s", wtpid, peekUserId());
				return null;	
			}
			
			// Found it! 
			//Util.pf("\nFound expack for %s, segment is %s", peekUserId(), peekExPack().segments);
			return peekExPack();
		}
		
		
		String peekUserId()
		{
			Util.massert(!theQ.isEmpty(), "Queue is empty");
			return theQ.peek()._1;	
		}
		
		ExPack peekExPack()
		{
			return theQ.peek()._2;
		}
		
		void pollCheck() throws IOException
		{
			theQ.poll();
			
			if(theQ.isEmpty()) // Don't call size() on LinkedList!!
				{ refillQ(); }
		}
		
		void refillQ() throws IOException
		{
			// Refill
			for(int i = 0; i < TARG_QSIZE; i++)
			{
				String line = getNextLine();
				
				if(line == null)
				{
					Util.pf("\nFinished reading all Exelate partition files");	
					break;
				}
				
				String[] toks = line.trim().split("\t");
				
				String wtpid = toks[2];
				ExPack expck = new ExPack(toks);
				
				theQ.add(Pair.build(wtpid, expck));
			}			
			
			//Util.pf("\nFinished refilling QUEUE, size is %d", theQ.size());
		}
		
		String getNextLine() throws IOException
		{
			if(curPartId == partTotal)
				{ return null; }
			
			String next = curReader.readLine();
			
			if(next == null)
			{
				Util.pf("\nFinished reading Exelate partition %s", Util.padLeadingZeros(curPartId, 5));
				curReader.close();
				curPartId++;

				if(curPartId < partTotal)
				{
					Util.sprintf("\nFinished searching partid %d", curPartId);
					curReader = HadoopUtil.hdfsBufReader(fSystem, partPath(curPartId));					
					next = curReader.readLine();					
				}
			}
						
			return next;
		}
		
		// Just for the sake of cleanliness
		public void doneReading()
		{
			try { curReader.close(); }
			catch (IOException ioex) { throw new RuntimeException(ioex); } 
		}
		
		static String partPath(int partid)
		{
			return Util.sprintf("/userindex/exelate/shards/part-%s", Util.padLeadingZeros(partid, 5));
		}
		
		public void restart()
		{
			throw new RuntimeException("not used");	
		}
		
		public int getWriteCount()
		{
			throw new RuntimeException("not used");	
		}
		
		public void flush()
		{
			throw new RuntimeException("not used");	
		}
		
		public void feedLine(String line)
		{
			throw new RuntimeException("not used");	
			
		}
	}	
}
