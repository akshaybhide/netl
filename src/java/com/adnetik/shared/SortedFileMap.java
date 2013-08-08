
package com.adnetik.shared;

import java.util.*;
import java.io.*;

import java.text.SimpleDateFormat;

import com.adnetik.shared.Util.*;

public class SortedFileMap
{
	public static final int DEFAULT_TARG_SIZE = 10000;
	
	private BufferedReader _bRead;
	
	private TreeMap<String, List<String>> _dataMap = Util.treemap();
	
	private int _targSize; 
	
	private boolean _doneReading = false;
	
	private String _delim = "\t";
	private int _idCol;
	
	private int _slurpLines = 0;
	
	private boolean _peelFirst;
	
	public SortedFileMap(BufferedReader bread) throws IOException
	{
		this(bread, DEFAULT_TARG_SIZE);
	}
	
	public SortedFileMap(BufferedReader bread, int tsize) throws IOException
	{
		this(bread, tsize, 0);
	}
	
	public SortedFileMap(BufferedReader bread, int tsize, int idcol) throws IOException
	{
		this(bread, tsize, idcol, false);
	}
	
	public SortedFileMap(BufferedReader bread, int tsize, int idcol, boolean pf) throws IOException
	{
		Util.massert(bread != null, "Reader is null");
		_bRead = bread;
		_targSize = tsize;
		_idCol = idcol;
		_peelFirst = pf;
		refQ();		
	}
	
	public static SortedFileMap buildFromFile(File toread, boolean dosort) throws IOException
	{
		return buildFromFile(toread, DEFAULT_TARG_SIZE, dosort);	
	}
		
	public static SortedFileMap buildFromFile(File toread, int tsize, boolean dosort) throws IOException
	{
		if(dosort)
		{
			Util.unixsort(toread.getAbsolutePath(), "");	
		}
		
		BufferedReader bread = FileUtils.getReader(toread.getAbsolutePath());
		return new SortedFileMap(bread, tsize);
		
	}
	
	public void setPeelFirst(boolean pf)
	{
		_peelFirst = pf;	
	}
	
	private void refQ() throws IOException
	{		
		// Util.pf("Trying to refresh, size is %d\n", _dataMap.size());
		
		while(!_doneReading && _dataMap.size() < _targSize)
		{
			String oneline = _bRead.readLine();
			
			if(oneline == null)
			{
				_doneReading = true;	
				_bRead.close();
				continue;
			}
			
			_slurpLines++;
			
			// Util.pf("Read line :\n%s\n", oneline);
			
			String linekey = getLineKey(oneline);
			if(linekey == null)
				{ continue; }
			
			Util.massert(_dataMap.isEmpty() || _dataMap.lastKey().compareTo(linekey) <= 0,
				"IDs read out of order: \ncur is '%s'\nnew is '%s'", 
				(_dataMap.isEmpty() ? "" : _dataMap.lastKey()), linekey);
			
			Util.setdefault(_dataMap, linekey, new LinkedList<String>());
			
			// Peel off the first record. This is usually used if 
			// you want to emulate Hadoop, Key/Iterator<?> pairs 
			if(_peelFirst)
				{  oneline = Util.splitOnFirst(oneline, _delim)._2; }

			_dataMap.get(linekey).add(oneline);
		}
		
		// This can happen if there is a long sequence of records with identical linekey
		Util.massert(_dataMap.size() > 1 || _doneReading,
			"DataMap size is %d, but not done reading", _dataMap.size());
	}
	
	private String getLineKey(String oneline)
	{
		if(_idCol == 0)
		{
			int delind = oneline.indexOf(_delim);
			return (delind == -1 ? null : oneline.substring(0, delind));
		}
		
		String[] toks = oneline.split(_delim);
		return toks.length <= _idCol ? null : toks[_idCol];
	}
	
	public Map.Entry<String, List<String>> pollFirstEntry() throws IOException
	{
		Util.massert(!_dataMap.isEmpty(), "Datamap is empty, you must check using hasNext before calling pollFirstEntry");
		Map.Entry<String, List<String>> mentry = _dataMap.pollFirstEntry();
		refQ();
		return mentry;
	}
	
	public String firstKey()
	{
		return _dataMap.firstKey();	
	}
		
	public void close() throws IOException
	{
		if(!_doneReading)
		{
			_doneReading = true;	
			_bRead.close();				
		}
	}
	
	public boolean hasNext()
	{
		return !_dataMap.isEmpty();	
	}
	
	public boolean isEmpty()
	{
		return _dataMap.isEmpty();	
	}
	
	public static void main(String[] args) throws IOException
	{
		File myfile = new File("slurp_2012-10-15.txt");
		SortedFileMap sfm = SortedFileMap.buildFromFile(myfile, false);
		int numpoll = 0;
		
		while(sfm.hasNext())
		{
			sfm.pollFirstEntry();
			
			numpoll++;
			
			if((numpoll % 10000) == 0)
			{
				Util.pf("Polled %s users, %s lines, nextkey=%s\n", 
					Util.commify(numpoll), Util.commify(sfm._slurpLines), sfm.firstKey());	
			}
		}
		
		sfm.close();
	}
}

