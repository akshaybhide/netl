
package com.adnetik.slicerep;

import java.util.*;
import java.io.*;
import java.sql.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;
import com.adnetik.slicerep.SliUtil.*;
import com.adnetik.shared.BidLogEntry.*;


public abstract class ReporterPack
{
	final Map<AggType, SortedSet<DimCode>> _dimMap = Util.conchashmap();
	
	public abstract void close() throws IOException;
	public abstract void writeKv(AggType atype, DumbDimAgg dagg, Metrics magg) throws IOException;
	public abstract void send2KvUploader(AggType atype, UploaderInterface kvup) throws IOException;
	
	ReporterPack(Map<AggType, SortedSet<DimCode>> dmap)
	{
		// This is all kind of paranoid, but weirder things have happened
		for(AggType atype : dmap.keySet())
		{
			_dimMap.put(atype, new TreeSet<DimCode>());
			_dimMap.get(atype).addAll(dmap.get(atype));
		}
	}
	
	// Write to disk, but instead of using Unix sort to group records with the same agg key,
	// send records to many different shard files, then read each shard file into memory and sort there. 
	// This works because each shard file is relatively smaller
	public static class ShardPack extends ReporterPack
	{
		public static final int NUM_SHARD = 100;
		
		private Map<AggType, List<BufferedWriter>> _writeMap;
		
		private boolean _isClosed;
		public static boolean IS_GZIP = true;
		
		public ShardPack(Map<AggType, SortedSet<DimCode>> dmap)
		{
			super(dmap);
		}
		
		private void initWriters() throws IOException
		{
			_writeMap = Util.treemap();
			
			for(AggType atype : AggType.values())
			{
				Util.setdefault(_writeMap, atype, new Vector<BufferedWriter>());
				
				for(int i : Util.range(NUM_SHARD))
				{ 
					String shardpath = getShardPath(atype, i);
					
					BufferedWriter bwrite = (IS_GZIP ? 
						FileUtils.getGzipWriter(shardpath) : FileUtils.getWriter(shardpath));
					
					_writeMap.get(atype).add(bwrite);
				}
			}
			
			_isClosed = false;			
		}
		
		public static String getShardPath(AggType atype, int sid)
		{
			return Util.sprintf("%s/shardpack/shard_%s_%d.txt%s", SliUtil.BASE_PATH, atype, sid, (IS_GZIP ? ".gz" : ""));
		}
		
		public synchronized void close() throws IOException
		{
			for(List<BufferedWriter> writelist : _writeMap.values())
			{
				for(BufferedWriter onebuf : writelist)
					{ onebuf.close(); }
			}
			
			_isClosed = true;
		}
		
		public void writeKv(AggType atype, DumbDimAgg dagg, Metrics magg) throws IOException
		{
			String val = magg.toQueryStr();
			String key;
			
			// Okay, the map lookup is synched because the map is a conchashmap
			// Then we synchronize on the dimension set.
			SortedSet<DimCode> dimset = _dimMap.get(atype);
			synchronized (dimset)
				{ key = dagg.computeKey(dimset, false); }
					
			int shardid = Math.abs(key.hashCode()) % NUM_SHARD;
			BufferedWriter bwrite = gimmeWriter(atype, shardid);
			
			synchronized ( bwrite )
			{
				bwrite.write(key);
				bwrite.write("\t");
				bwrite.write(val);
				bwrite.write("\n");				
			}			
		}
		
		private synchronized BufferedWriter gimmeWriter(AggType atype, int shardid) throws IOException
		{
			if(_writeMap == null)
				{ initWriters(); }	
			
			return _writeMap.get(atype).get(shardid);
		}
		
		public void send2KvUploader(AggType atype, UploaderInterface kvup) throws IOException
		{
			Util.massert(_isClosed, "Attempt to send data before closing ReporterPack");
			
			for(int i : Util.range(NUM_SHARD))
				{ sendOneFile(getShardPath(atype, i), atype, kvup); }
			
			kvup.finish();
		}
		
		private void sendOneFile(String shardpath, AggType atype, UploaderInterface kvup) throws IOException
		{
			int lcount = 0;
			TreeMap<String, Metrics> smallmemmap = Util.treemap();
			
			BufferedReader bread = (IS_GZIP ? Util.getGzipReader(shardpath) : Util.getReader(shardpath));
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				if(oneline.trim().length() == 0)
					{ continue; }
				
				String[] key_val = oneline.split("\t");
				Metrics magg = Metrics.fromQueryStr(key_val[1]);
				
				Util.setdefault(smallmemmap, key_val[0], new Metrics());
				smallmemmap.get(key_val[0]).add(magg);
				
				lcount++;
			}
			bread.close();
			
			// Util.pf("Read %d lines for file %s, mapsize is %d\n", lcount, shardpath, smallmemmap.size());
			
			for(String onekey : smallmemmap.keySet())
			{
				kvup.send(atype, BmUtil.getParseMap(onekey), smallmemmap.get(onekey));
			}
			
			
		}
	}
	
	// Store all the aggregation data in memory
	public static class MemoryPack extends ReporterPack
	{
		int preaggCount = 0;
		
		Map<AggType, SortedMap<String, Metrics>> _memMap = Util.treemap();
		

		public MemoryPack(Map<AggType, SortedSet<DimCode>> dmap)
		{
			super(dmap);
			
			for(AggType atype : AggType.values())
			{ 
				SortedMap<String, Metrics> onemap = Util.treemap();
				_memMap.put(atype, onemap);
			}			
		}
		
		public synchronized void close() throws IOException {}
		
		public synchronized void writeKv(AggType atype, DumbDimAgg dagg, Metrics magg) throws IOException
		{
			SortedMap<String, Metrics> onemap = _memMap.get(atype);
			String key = dagg.computeKey(_dimMap.get(atype), false);
			
			// Classic bug!! Previous code simply placed
			// the Magg reference passed as an argument into the map.
			// This meant that it was being
			if(!onemap.containsKey(key))
			{	
				Metrics newmet = new Metrics();	
				onemap.put(key, newmet);
			}
			
			onemap.get(key).add(magg);
			
			preaggCount++;
		}
		
		// This and sumGetField(..) are used only for debugging, maybe take out
		public synchronized void checkMetData()
		{
			for(int i = 0; i < IntFact.values().length; i++)
			{
				int a = sumGetField(AggType.ad_domain, IntFact.values()[i]);
				int b = sumGetField(AggType.ad_general, IntFact.values()[i]);
				Util.massert(a == b, "Inconsistent for IntFact %s", IntFact.values()[i]);
			}
		}
		
		private synchronized int sumGetField(AggType atype, IntFact ifact)
		{
			int total = 0;
			
			for(String key : _memMap.get(atype).keySet())
			{
				Metrics met = _memMap.get(atype).get(key);
				total += met.getField(ifact);
			}			
			return total;
		}
		
		public void send2KvUploader(AggType atype, UploaderInterface kvup) throws IOException
		{			
			SortedMap<String, Metrics> onemap = _memMap.get(atype);			
			
			for(String onekey : onemap.keySet())
			{
				kvup.send(atype, BmUtil.getParseMap(onekey), onemap.get(onekey));
			}
			
			kvup.finish();
		}
	}
	
	// Use the disk as the data store, don't try to fit in Memory
	// TODO: do I really need THREE implementations, two are based on disk writing...?
	public static class WriterPack extends ReporterPack
	{
		Map<AggType, Writer> _writeMap = Util.treemap();

		public WriterPack(Map<AggType, SortedSet<DimCode>> dmap)
		{
			super(dmap);	
		}
		
		public synchronized void close() throws IOException
		{
			for(Writer onewrite : _writeMap.values())
				{ onewrite.close(); }
		}
		
		public synchronized void writeKv(AggType atype, DumbDimAgg dagg, Metrics magg) throws IOException
		{
			String key = dagg.computeKey(_dimMap.get(atype), false);
			String val = magg.toString("&");
			
			writeKv(atype, key, val);			
		}
		
		// This method is SINGLE-THREADED 
		public void send2KvUploader(AggType atype, UploaderInterface kvup) throws IOException
		{
			Util.unixsort(SliUtil.getKvTempPath(atype), SliUtil.TEMP_KV_PATH_GIMP, "");
			
			BufferedReader src = Util.getReader(SliUtil.getKvTempPath(atype));
			String curkey = null;
			Metrics curmet = null;
			
			for(String line = src.readLine(); line != null; line = src.readLine())
			{
				String[] dim_fct = line.split("\t");
				
				String newkey = dim_fct[0];
				String newval = dim_fct[1];
				
				if(!newkey.equals(curkey))
				{
					outputCurData(atype, curkey, curmet, kvup);
					
					// Create new Metrics with zero values.
					curmet = new Metrics();
					curkey = newkey;
				}
				
				Metrics newmet = Metrics.fromQueryStr(newval);
				curmet.add(newmet);		
			}
			
			// Call one final time
			outputCurData(atype, curkey, curmet, kvup);
			
			src.close();
			kvup.finish();
			
			Util.pf("Finished aggregation, %d output, %d uploaded successfully lines\n", 
				 kvup.getTotalIn(), kvup.getTotalUp());
		}
		
		private void outputCurData(AggType atype, String onekey, Metrics onemet, UploaderInterface kvup) throws IOException
		{
			if(onekey != null)
				{ kvup.send(atype, BmUtil.getParseMap(onekey), onemet); }
		}		
		
		synchronized void writeKv(AggType atype, String key, String val) throws IOException
		{
			if(!_writeMap.containsKey(atype))
			{
				Writer awrite = new BufferedWriter(new FileWriter(SliUtil.getKvTempPath(atype)));	
				_writeMap.put(atype, awrite);
			}			
			
			Writer mywriter = _writeMap.get(atype);
			mywriter.write(key);
			mywriter.write("\t");
			mywriter.write(val);
			mywriter.write("\n");
		}
	}
}
