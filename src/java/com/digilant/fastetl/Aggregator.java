package com.digilant.fastetl;

import java.util.*;
import java.io.*;

import com.adnetik.bm_etl.BleStructure;
import com.adnetik.bm_etl.DumbDimAgg;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util;
import com.digilant.fastetl.FastUtil.*;

public class Aggregator
{
	FastUtil.MyLogType _relType;
	
	private Map<Integer, Integer> _bigMap = Util.hashmap();
	
	// TODO: worried about memory 
	// private TreeMap<WtpId, InfoPack> wtpIdAgg = Util.treemap();

	// private HashMap<Integer, Integer> lineOrPixAgg = Util.hashmap();
	
	public Aggregator(FastUtil.MyLogType ltype)
	{
		_relType = ltype;
	}	
	
	void incForId(int relid)
	{
		Util.massert(!(_relType == MyLogType.pixel && relid > 30000), "Found relid %d for pixel agg", relid);
		
		int c = _bigMap.containsKey(relid) ? _bigMap.get(relid) : 0;
		_bigMap.put(relid, c+1); 
	}
	
	public Set<Integer> getIdSet()
	{
		return _bigMap.keySet();	
	}
	
	public Integer getCount(int relid)
	{
		return _bigMap.get(relid);
	}

	void setCount(int relid, int count)
	{
		_bigMap.put(relid, count);		
	}

}

