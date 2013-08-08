
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.data_management.*;

public class UserPack
{	
	String userId;
	
	public static final String ID_NOT_SET = "UnknownUserId";
	
	private List<BidLogEntry> _bidList = Util.vector();
	private List<PixelLogEntry> _pixList = Util.vector();
	
	private Map<LogField, String> _modeCache = Util.treemap();
	
	private Map<LogField, Integer> _varCache = Util.treemap();
	
	Map<String, Set<String>> uniqCache = Util.treemap();
	
	private boolean _readExel = false;
	private boolean _readBlue = false;
	
	private static boolean _READ_BLUE = false;
	private static boolean _READ_EXEL = false;
	
	private ExelateDataMan.ExUserPack _exelPack;
	private BluekaiDataMan.BluserPack _bluePack;
	
	public void printMe()
	{
		System.out.printf("\nUser data id: %s", userId);
		
		for(BidLogEntry ble : _bidList)
		{
			System.out.printf("\n\t%s", ble);
		}
	}
	
	public List<BidLogEntry> getBidList()
	{
		return Collections.unmodifiableList(_bidList);	
	}	
	
	public List<PixelLogEntry> getPixList()
	{
		return Collections.unmodifiableList(_pixList);
	}
	
	public int getBidCount()
	{
		return 	_bidList.size();
	}
	
	public int getPixCount() 
	{
		return _pixList.size();	
	}
	
	// Use same add method for both pixel and bid logs
	// depends on wtp_user_id having the same name in both
	// log types
	public void add(LogEntry logent)
	{
		add(logent, LogField.wtp_user_id);
	}
	
	public void add(LogEntry logent, LogField idfieldname)
	{
		String newid = logent.getField(idfieldname).toLowerCase();
		
		if(userId == null)
		{
			userId = newid; 
		} 
		
		Util.massert(userId.equals(newid), "Attempt to add ID %s to UserPack with ID %s",
			newid, userId);
		
		if(logent instanceof BidLogEntry)
			{ _bidList.add((BidLogEntry) logent); }
		else 
			{ _pixList.add((PixelLogEntry) logent); }
	}
	
	int getFieldDiversity(LogField fname)
	{
		if(!_varCache.containsKey(fname))
		{
			Set<String> fset = Util.treeset();
			
			for(BidLogEntry ble : _bidList)
			{
				fset.add(ble.getField(fname));
			}
			
			_varCache.put(fname, fset.size());
		}
		
		return _varCache.get(fname);
	}

	public String getFieldMode(LogField fname)
	{
		if(!_modeCache.containsKey(fname))
		{
			Map<String, Integer> countmap = Util.treemap();
			
			for(BidLogEntry ble : _bidList)
			{
				String f = ble.getField(fname).trim();	
							
				if(f.length() > 0)
					{ Util.incHitMap(countmap, f); }
			}			
			
			String result = "";
			int topval = 0;
			
			for(String onekey : countmap.keySet())
			{
				if(countmap.get(onekey) > topval)
				{
					topval = countmap.get(onekey);	
					result = onekey;
				}
			}
		
			_modeCache.put(fname, result);
		}
		
		return _modeCache.get(fname);
	}
	
	public BluekaiDataMan.BluserPack getBluePack() throws IOException
	{
		// Util.pf("Calling getbluepack for %s, queuenext=%s, readblue=%b\n", userId, BluekaiDataMan.getSingQ().peekNextId(), _readBlue);
		
		if(!_readBlue)
		{
			// Util.pf("Calling getbluepack for %s\n", userId);
			_bluePack = (BluekaiDataMan.isQReady() ? BluekaiDataMan.getSingQ().lookup(userId) : null);
			_readBlue = true;
		}
		
		return _bluePack;
	}
	
	public ExelateDataMan.ExUserPack getExelatePack() throws IOException
	{
		if(!_readExel)
		{		
			// Only read the thing if the DataMan is initialized, otherwise just skip
			_exelPack = (ExelateDataMan.isQReady() ? ExelateDataMan.getSingQ().lookup(userId) : null);
			_readExel = true;
		}
		
		return _exelPack;
	}
}
