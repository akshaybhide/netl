
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
//import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.data_management.SegmentPathMan.*;
import com.adnetik.data_management.SegmentPack.*;


// This class mirrors structure of 
public abstract class SegmentDataQueue
{
	
	TreeMap<String, List<String>> _datamap = Util.treemap();

	LineReader _mastReader; 
	boolean _doneReading = false;
	
	public int polledUsers = 0;
	public int linesRead = 0;			
	
	// TODO: is this large enough?
	public static int TARG_SIZE = 1000;
	
	private double _lookupTimeMillis = 0;
	private double _lastLookupTimeMillis = 0;
		
	Party3Type _dataSetCode;
	
	SegmentDataQueue(Party3Type datacode, LineReader bread) throws IOException
	{
		_mastReader = bread;
		refQ();	
		
		_dataSetCode = datacode;
	}
	
	SegmentDataQueue(Party3Type datacode) throws IOException
	{
		_dataSetCode = datacode;
	}	
	
	void initFromHdfs(String daycode) throws IOException
	{
		BufferedReader bread = getPathMan().getHdfsMasterReader(daycode);
		_mastReader = FileUtils.bufRead2Line(bread);
		
		refQ();
	}
	
	void initFromLocal(String daycode) throws IOException
	{
		BufferedReader bread = getPathMan().getLocalMasterReader(daycode);
		_mastReader = FileUtils.bufRead2Line(bread);		
		refQ();
	}	
	
	private void refQ() throws IOException
	{
		Util.massert(_mastReader != null, "Must initialize Master-reader");
		
		while(_datamap.size() < TARG_SIZE && !_doneReading)
		{
			String oneline = _mastReader.readLine();
			
			if(oneline == null || oneline.trim().length() == 0)
			{
				_doneReading = true;
				_mastReader.close(); 
				continue;					
			}
			
			linesRead++;
			
			int tabind = oneline.indexOf("\t");
			if(tabind == -1)
				{ continue; }
			String linekey = oneline.substring(0, tabind);
			
			Util.setdefault(_datamap, linekey, new Vector<String>());
			_datamap.get(linekey).add(oneline);
		}
	}
	
	public Party3Type getDataSet()
	{
		return _dataSetCode;	
	}
	
	public SegmentPathMan getPathMan()
	{
		return new SegmentPathMan(_dataSetCode, true);
	}
	
	public double lastLookupTimeSecs()
	{
		return _lastLookupTimeMillis/1000;
	}
	
	public long lastLookupTimeMillis()
	{
		return Math.round(_lastLookupTimeMillis);
	}
	
	public double totLookupTimeSecs()
	{
		return _lookupTimeMillis/1000;
	}
		
	public String peekNextId()
	{
		return (_datamap.isEmpty() ? null : _datamap.firstKey());
	}
	
	public void close() throws IOException
	{
		if(!_doneReading)
		{
			_mastReader.close();
			_doneReading = true;
		}
	}
	
	public boolean hasNext()
	{
		// Util.pf("Data map size is %d\n", _datamap.size());
		return !_datamap.isEmpty();
	}		
	
	// Faster version of nextPack that doesn't build the BluserPack
	void discardNextPack() throws IOException
	{
		_datamap.pollFirstEntry();
		refQ();
		polledUsers++;
	}
	
	Map.Entry<String, List<String>> lookupEntry(String userid) throws IOException
	{
		double startlook = Util.curtime();
		
		Map.Entry<String, List<String>> cookiepack = lookupEntrySub(userid);
		
		_lastLookupTimeMillis = (Util.curtime() - startlook);
		_lookupTimeMillis += _lastLookupTimeMillis;
		return cookiepack;		
	}
	
	Map.Entry<String, List<String>> lookupEntrySub(String userid) throws IOException
	{
		// Bypass these without actually building the BlueUserPac
		while(hasNext() && peekNextId().compareTo(userid) < 0)
			{ discardNextPack(); }
		
		return userid.equals(peekNextId()) ? nextEntry() : null;
	}	
	
	Map.Entry<String, List<String>> nextEntry() throws IOException
	{
		if(!hasNext())
			{ return null; }
		
		Map.Entry<String, List<String>> cookiepack = _datamap.pollFirstEntry();
		refQ();
		polledUsers++;
		return cookiepack;
	}
	
	public abstract SegmentPack nextPack() throws IOException;
	
	public abstract SegmentPack buildEmpty(String wtpid);
	
	public int getPollUserCount() { return polledUsers; }
}
