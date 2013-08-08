
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import  java.nio.*;
import  java.nio.channels.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class SegmentLookup
{
	Path _targPath;
	FileSystem _fSystem;
	
	int _probeCount;
	int _indexProbeCount;
	
	Random _jRand = new Random();
	
	private StreamWrapper _sWrap;
	
	private TreeMap<String, Long> _indexMap = Util.treemap();
	
	public static void main(String[] args) throws IOException
	{

		
		List<String> idlist; 
		File outputfile;
		String p3code;
		String daycode;
		
		try {
			idlist = FileUtils.readFileLinesE(args[0]);
			outputfile = new File(args[1]);
			
			p3code = args[2];
			
			Util.massert(p3code.equals("bluekai") || p3code.equals("exelate"),
				"Bad p3code %s", p3code);
			
			daycode = args[3];
			TimeUtil.assertValidDayCode(daycode);
		} catch (Exception ex) {
			
			Util.pf("Usage: <idlist> <outputfile> <bluekai|exelate> <daycode>\n");
			return;
		}
			
		ArgMap amap = Util.getClArgMap(args);
		int indexsize = amap.getInt("indexsize", 10);

		SegmentLookup slook = new SegmentLookup(p3code, daycode);
		slook.buildIndex(indexsize);
		slook.writeSliceFile(idlist, outputfile);
		
	}
	
	void writeSliceFile(List<String> idlist, File output) throws IOException
	{
		double startup = Util.curtime();
		int notfound = 0;
		int lookuphits = 0;
		
		BufferedWriter bwrite = FileUtils.getWriter(output.getAbsolutePath());
		
		for(int i = 0; i < idlist.size(); i++)
		{
			String oneid = idlist.get(i);
			
			String sid = oneid.trim();
			if(sid.length() == 0)
				{ continue; }
			
			long foundpos = binSearch(sid);
			
			if(foundpos == -1)
			{ 
				// Util.pf("Failed to find ID %s\n", oneid);
				notfound++;
				continue; 
			}
			
			Pair<String, List<String>> data4id = getRows4Pos(foundpos);
			FileUtils.writeRow(bwrite, "\n", data4id._2);
			
			
			lookuphits++;			
			
			if((i % 100) == 0)
			{
				double lookpersec = (i+1)/((Util.curtime()-startup)/1000);
				
				Util.pf("Finished lookup %d/%d, %d probes, found %d IDs, could not find %d, took %.03f, %.03f lookup/sec\n",
					i, idlist.size(), _probeCount,
					lookuphits, notfound, (Util.curtime()-startup)/1000, lookpersec);
			}			
		}		
		
		bwrite.close();
		
		Util.pf("Timing test complete, found %d IDs, could not find %d, took %.03f\n",
			lookuphits, notfound, (Util.curtime()-startup)/1000);		
		
	}
	
	void timingTest(List<String> idlist) throws IOException
	{
		double startup = Util.curtime();
		int notfound = 0;
		int lookuphits = 0;
		
		for(int i = 0; i < idlist.size(); i++)
		{
			if((i % 100) == 0)
			{
				double lookpersec = (i+1)/((Util.curtime()-startup)/1000);
				
				Util.pf("Finished lookup %d/%d, %d probes, found %d IDs, could not find %d, took %.03f, %.03f lookup/sec\n",
					i, idlist.size(), _probeCount,
					lookuphits, notfound, (Util.curtime()-startup)/1000, lookpersec);

				Util.showMemoryInfo();			
			}			
			
			
			String oneid = idlist.get(i);
			
			String sid = oneid.trim();
			if(sid.length() == 0)
				{ continue; }
			
			long foundpos = binSearch(sid);
			
			if(foundpos == -1)
			{ 
				// Util.pf("Failed to find ID %s\n", oneid);
				notfound++;
				continue; 
			}
			
			Pair<String, List<String>> data4id = getRows4Pos(foundpos);
			
			lookuphits++;			
			

		}		
		
		Util.pf("Timing test complete, found %d IDs, could not find %d, took %.03f\n",
			lookuphits, notfound, (Util.curtime()-startup)/1000);
	}
	
	public SegmentLookup(String p3c, String daycode) throws IOException
	{
		String tp = Util.sprintf("/thirdparty/%s/snapshot/MASTER_LIST_%s.txt",
					p3c.toString().toLowerCase(), daycode);
		
		_targPath = new Path(tp);
		_fSystem = FileSystem.get(new Configuration());
		
		_sWrap = new SingleReadWrapper(_fSystem, _targPath);
		// _sWrap = new LocalFileWrapper("/local/fellowship/thirdparty/bluekai/MASTER_LIST_2013-04-15.txt");
		
	}
	


	
	// Okay, ensure that getNextTransitionPos() and getRows4Pos return the same data
	private void testRandomLookup() throws IOException
	{
		int lastprobe = (_sWrap.getTotalSize() > Integer.MAX_VALUE ? Integer.MAX_VALUE : (((int) _sWrap.getTotalSize())-10000));
		
		Util.pf("Last probe position is %d\n", lastprobe);
		
		for(int i = 0; i < 100; i++)
		{
			long randpos = _jRand.nextInt(lastprobe);
			_sWrap.seek(randpos);
			
			Pair<String, Long> transpair = getNextTransitionPos();
			
			Pair<String, List<String>> rows4id = getRows4Pos(transpair._2);
			
			Util.pf("Pair ID is %s, row ID is %s\n", 
				transpair._1, rows4id._1);
			
			Util.massert(transpair._1.equals(rows4id._1),
				"Found transpair %s, row data %s", 
				transpair._1, rows4id._1);
			
			// Util.pf("Randpos is %d, pairpos is %d\n", randpos, transpair._2);
			
			// The transpair number must be the position of the start of the newline
			checkNewLinePos(transpair._2-1);
			
			
		}
		
	}	
	
	
	// TODO: this affects the current position, probably not such a good idea
	private void checkNewLinePos(long shouldbenewline) throws IOException
	{
		// "Virtual" new line
		if(shouldbenewline == -1)
			{ return; }
		
		_sWrap.seek(shouldbenewline);
		
		int c = _sWrap.readSingle();
		
		Util.massert(c == 10, "Expected newline ASCII 10 but found %d", c);
	}
	
	// Gets the ID, position of the next TRANSITION, where one ID range becomes another.
	private Pair<String, Long> getNextTransitionPos() throws IOException
	{
		// throwaway
		_sWrap.readLine();
		
		String curid = null;
		
		for(int i = 0; i < 1000; i++)
		{
			// This is the exact position of the BEGINNING of the line
			long linepos = _sWrap.getPos();
			
			String oneline = _sWrap.readLine();

			String newid = oneline.split("\t")[0];			
			
			if(curid == null)
				{ curid = newid; }
			
			if(!curid.equals(newid))
			{
				return Pair.build(newid, linepos);
			}
		}
	
		Util.massert(false, "No ID transition found after 1000 lines");
		return null;
		
	}
	
	// Go to the given position, read rows until we hit an ID transition point
	private Pair<String, List<String>> getRows4Pos(long startpos) throws IOException
	{
		if(_jRand.nextDouble() < .001)
			{ checkNewLinePos(startpos-1); }
		
		List<String> seglist = Util.vector();
		String id = null;
		
		_sWrap.seek(startpos);
		
		for(String oneline = _sWrap.readLine(); oneline != null; oneline = _sWrap.readLine())
		{
			String[] toks = oneline.split("\t");
			
			String newid = toks[0];
			
			if(id == null)
				{ id = newid; }
			
			if(!id.equals(newid))
				{ break; }
			
			seglist.add(oneline);
		}
	
		return Pair.build(id, seglist);
	}
	
	// Height is eg 16, for 65K index entries
	public void buildIndex(int height) throws IOException
	{
		double startup = Util.curtime();
		recBuildIndex(0L, _sWrap.getTotalSize(), height);
		Util.pf("Built index with height %d, %d entries, took %.03f, %d probes\n",
			height, _indexMap.size(), (Util.curtime()-startup)/1000, _indexProbeCount);
	}
	
	
	private void recBuildIndex(long min, long max, int height) throws IOException
	{
		if(height == 0)
			{ return; }
		
		long mid = (max+min)/2;
		
		_sWrap.seek(mid);
		Pair<String, Long> probepair = getNextTransitionPos();
		_indexProbeCount++;
		
		_indexMap.put(probepair._1, probepair._2);

		recBuildIndex(min, mid, height-1);
		recBuildIndex(mid, max, height-1);
	}
	
	
	
	long binSearch(String wtptarg) throws IOException
	{
		// Never a good reason not to use at least a small index
		if(_indexMap.isEmpty())
			{ buildIndex(4); }
		
		Map.Entry<String, Long> florent = _indexMap.floorEntry(wtptarg);
		Map.Entry<String, Long> highent = _indexMap.higherEntry(wtptarg);

		if(florent != null && florent.getKey().equals(wtptarg))
			{ return florent.getValue(); }
		
		if(highent != null && highent.getKey().equals(wtptarg))
			{ return highent.getValue(); }
				
		return binSearchSub(wtptarg, 
			(florent == null ? 0 			 : florent.getValue()),
			(highent == null ? _sWrap.getTotalSize() : highent.getValue()));
	}
	
	private long binSearchSub(String idtarg, long min, long max) throws IOException
	{
		
		if(max - min < 50)
		{
			// Util.pf("Min=%d, Max=%d but targ not found, search failed\n", min, max);
			return -1;
		}
		
		long mid = (max+min)/2;
		
		_sWrap.seek(mid);
		Pair<String, Long> probepair = getNextTransitionPos();

		_probeCount++;
		
		// Util.pf("Probed position=%d, result is %s\n", mid, probe);
		
		int comp = idtarg.compareTo(probepair._1);
		
		if(comp < 0)
		{
			// targ is before probe	
			return binSearchSub(idtarg, min, mid);
		} else if(comp > 0) {
			
			return binSearchSub(idtarg, mid, max);	
		} else {
			// Util.pf("FOUND target id %s at position %d\n", wtptarg, mid);
			return probepair._2;	
		}
		
	}
	
	private abstract class StreamWrapper
	{
		
		public abstract long getPos() throws IOException;
		
		abstract int readSingle() throws IOException;
	
		abstract void seek(long newpos) throws IOException;
		
		public abstract long getTotalSize();
		
		abstract String readLine() throws IOException;

	}
	
	private class LocalFileWrapper extends StreamWrapper
	{
		RandomAccessFile _raFile;
		
		long _totalSize;		
		
		
		public LocalFileWrapper(String path) throws IOException
		{
			File f = new File(path);
			_raFile = new RandomAccessFile(f, "r");
			
			_totalSize = f.length();
		}
		
		
		public long getTotalSize()
		{
			return _totalSize;	
		}	
		
		public long getPos() throws IOException
		{
			return _raFile.getFilePointer();		
		}
		
		int readSingle() throws IOException
		{
			return _raFile.read();	
		}
	
		void seek(long newpos) throws IOException
		{
			_raFile.seek(newpos);		
		}
		
		String readLine() throws IOException
		{
			StringBuilder sb = new StringBuilder();
			
			for(int c = _raFile.read(); c != -1; c = _raFile.read())
			{
				// Better be ASCII!!!!
				
				char nc = (char) c;
				
				if(nc == '\n')
					{ break; }
				
				sb.append(nc);
			}
			
			return sb.toString();
		}			
		
		
	}
	
	private class SingleReadWrapper extends StreamWrapper
	{
		FSDataInputStream _segStream;
		long _totalSize;		

		SingleReadWrapper(FileSystem fsys, Path tpath) throws IOException
		{
			_segStream = fsys.open(tpath);
			_totalSize = fsys.getFileStatus(tpath).getLen();
			
			Util.pf("Initialized SLT, file size is %d\n", _totalSize);			
		}
		
		public long getPos() throws IOException
		{
			return _segStream.getPos();			
		}
		
		int readSingle() throws IOException
		{
			return _segStream.read();	
		}
	
		void seek(long newpos) throws IOException
		{
			_segStream.seek(newpos);			
		}
		
		public long getTotalSize()
		{
			return _totalSize;	
		}
		
		String readLine() throws IOException
		{
			StringBuilder sb = new StringBuilder();
			
			for(int c = _segStream.read(); c != -1; c = _segStream.read())
			{
				// Better be ASCII!!!!
				
				char nc = (char) c;
				
				if(nc == '\n')
					{ break; }
				
				sb.append(nc);
			}
			
			return sb.toString();
		}		
		
	}
	
	private class BufReadWrapper extends StreamWrapper
	{
		FSDataInputStream _segStream;
		
		long _totalSize;		

		byte[] _gimpBuf = new byte[1000];
		
		private long _lastRead = -1;
		private int _bOffset = -1;
		private int _maxBufValid = -1000;

		BufReadWrapper(FileSystem fsys, Path tpath) throws IOException
		{
			_segStream = fsys.open(tpath);
			_totalSize = fsys.getFileStatus(tpath).getLen();
			
			Util.pf("Initialized SLT, file size is %d\n", _totalSize);

			// This is kinda wasteful, but whatev
			slurp();			
		}
		
		public long getPos() throws IOException
		{
			return _lastRead + _bOffset;
		}
		

		
		private void slurp() throws IOException
		{
			// Remember position of last read
			_lastRead = _segStream.getPos();
			
			// What is the correct place to do this...?
			_maxBufValid = _segStream.read(_segStream.getPos(), _gimpBuf, 0, _gimpBuf.length);
			
			// We're now at the start of the buffer again
			_bOffset = 0;
		}
	
		void seek(long newpos) throws IOException
		{
			_segStream.seek(newpos);
			
			slurp();
		}
		
		public long getTotalSize()
		{
			return _totalSize;	
		}
		
		int readSingle() throws IOException
		{
			if(_bOffset == _maxBufValid)
				{ slurp(); }
			
			return _gimpBuf[_bOffset++];
		}		
		
		String readLine() throws IOException
		{
			StringBuilder sb = new StringBuilder();
			
			while(true)
			{
				Util.pf("Offset is %d, valid is %d\n", _bOffset, _maxBufValid);
				
				if(_bOffset == _maxBufValid)
					{ slurp(); }

				int c = _gimpBuf[_bOffset++];
				
				char nc = (char) c;
				
				if(nc == '\n')
					{ break; }
							
				sb.append(nc);				
			}
			
			return sb.toString();
		}		
		
	}	
	
	private static int b2i(byte b)
	{
		int x = (int) b;
		if(x < 0)
			{ x += 256; }
		
		return x;
	}	
	
}
