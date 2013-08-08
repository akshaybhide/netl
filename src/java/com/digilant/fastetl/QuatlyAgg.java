package com.digilant.fastetl;

import java.util.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.LogEntry;
import com.adnetik.shared.TimeUtil;
import com.adnetik.shared.Util;

import java.util.*;
import java.io.*;

import com.digilant.fastetl.FastUtil.*;

public class QuatlyAgg 
{
	
	//  Keys are date codes of the form: 
	// 2012-05-15 01:15:00
	// 2012-05-15 01:30:00
	// Size/day = 24*4*4
	FileManager _fileman;
	SortedMap<String, Map<MyLogType, Aggregator>> _lookupMap = Util.treemap();	
	public QuatlyAgg(FileManager f){
		_fileman = f;
		
	}
	boolean hasAggBlock(BlockCode bcode)
	{
		return _lookupMap.containsKey(bcode.toTimeStamp());
	}
	
	boolean processLogEntry(MyLogType mltype, LogEntry logent, Set<Integer> interestset)
	{
		int relid = FastUtil.getRelevantId(logent);	
		
		// Util.pf("Found data for lineitem %d\n", relid);
		// only add if the relevant id is in the interest set
		if(!interestset.contains(relid))
			{ return false; }
		
		String timestamp = logent.getField("date_time");
		BlockCode bcode = BlockCode.prevBlock(timestamp);
		
		if(!hasAggBlock(bcode))
		{ 
			// Util.pf("BCode timestamp is %s for timestamp %s\n", bcode.toTimeStamp(), timestamp);
			createAggBlock(bcode); 
		}


		findAggByBlock(bcode, mltype).incForId(relid);
		return true;
	}	
	
	void createAggBlock(BlockCode bcode)
	{
		Util.massert(!hasAggBlock(bcode));
		
		Map<MyLogType, Aggregator> mymap = Util.hashmap();
		
		for(MyLogType mlt : MyLogType.values())
		{
			mymap.put(mlt, new Aggregator(mlt));
		}
		
		_lookupMap.put(bcode.toTimeStamp(), mymap);	
	}	
	
	// TODO: probably a bad method, because it mixes Pixel IDs with Line Item IDs

	Set<Integer> compileIdSet(boolean ispixel)
	{
		Set<Integer> idset = Util.treeset();	
		for(Map<MyLogType, Aggregator> onemap : _lookupMap.values())
		{
			for(Aggregator agg : onemap.values())
			{
				if(ispixel && agg._relType == MyLogType.pixel){
					idset.addAll(agg.getIdSet());
				}
				else if(!ispixel && agg._relType != MyLogType.pixel)
					idset.addAll(agg.getIdSet());
			}
		}
		return idset;		
	}
	
	void writeToStaging() throws IOException
	{
		for(int oneid : compileIdSet(false))
		{
			for(MyLogType mlt : MyLogType.values())
				{ 
					if(mlt == MyLogType.pixel)
						continue;
					writeToStaging(mlt, oneid);
				}	
		}
		for(int oneid : compileIdSet(true))
		{
			writeToStaging(MyLogType.pixel, oneid);
		}
	}
	
	private void writeToStaging(MyLogType mlt, int relid) throws IOException
	{
		List<String> flines = Util.vector();
		
		for(String blockkey : _lookupMap.keySet())
		{
			Aggregator oneagg = _lookupMap.get(blockkey).get(mlt);
			if(oneagg == null) // Some aggregators might be empty
				{ continue;
				//oneagg = new QuatlyAgg();
				}
			
			BlockCode bcode = BlockCode.fromBlockKey(blockkey);
			Integer count = oneagg.getCount(relid);
			if(count == null)
				{ //continue;
				count = 0;
				}
		
			
			String quatline = Util.sprintf("%s\t%d", bcode.getQuatKey(), count);
			flines.add(quatline);
		}
		
		// write to staging (=false)
		if(!flines.isEmpty())
		{ 
			String quatpath = _fileman.getQuatlyPath(false, mlt, relid);
			FileUtils.createDirForPath(quatpath);
			FileUtils.writeFileLines(flines, quatpath); 
			String versionfilepath = quatpath + ".version";
			flines.clear();
			
			flines.add(FastUtil.getNowString());
			FileUtils.createDirForPath(versionfilepath);
			FileUtils.writeFileLines(flines, versionfilepath);
		}
	}
	
	
	Aggregator findAggByBlock(BlockCode bcode, FastUtil.MyLogType mtype)
	{
		return findAggregator(bcode.toTimeStamp(), mtype);
	}
		
	Aggregator findAggregator(String timestamp, FastUtil.MyLogType mtype)
	{
		// Precompute and save sttvalid/endvalid
		String sttvalid = FastUtil.prevBlock(_lookupMap.firstKey());
		String endvalid = _lookupMap.lastKey();
		
		if(timestamp.compareTo(sttvalid) < 0 || timestamp.compareTo(endvalid) > 0)
		{
			throw new RuntimeException("Time stamp out of range: " + timestamp + " start :" + sttvalid + " end : " + endvalid + "\n");	
		}
		
		
		String relkey = _lookupMap.tailMap(timestamp).firstKey();
		// Util.pf("Found lookup stt=%s ts=%s end=%s\n", FastUtil.prevBlock(relkey), timestamp, relkey);
		
		return _lookupMap.get(relkey).get(mtype);
	}
	
	void loadFromSaveData(Set<String> aggpathset) throws IOException
	{	
		for(String onepath : aggpathset)
		{
			if(!onepath.endsWith("quatly"))
				{ continue; }

			// smart parsing to get relid and logtype			
			File onefile = new File(onepath);
			int relid = Integer.valueOf(onefile.getParentFile().getName());
			MyLogType mlt = MyLogType.valueOf(onefile.getName().split("\\.")[0]);
			
			Scanner sc = new Scanner(new File(onepath));
			while(sc.hasNext())
			{
				BlockCode bcode = new BlockCode(sc.next(), sc.nextInt(), sc.nextInt());
				
				if(!hasAggBlock(bcode))
					{ createAggBlock(bcode); }
				
				Aggregator agg = findAggByBlock(bcode, mlt);
				agg.setCount(relid, sc.nextInt());
				
			}
			sc.close();				
		}
	}		

	void initAggregators(String date, int numdays)
	{
		List<String> dayrange = TimeUtil.getDateRange(/*Util.getTodayCode()*/Util.findDayCode(date), numdays);
		for(String daycode : dayrange)
		{
			for(int hr = 0; hr < 24; hr++)
			{
				for(int qt = 0; qt < 4; qt++)
				{
					BlockCode bcode = new BlockCode(daycode, hr, qt);
					_lookupMap.put(bcode.toTimeStamp(), new TreeMap<FastUtil.MyLogType, Aggregator>());
					//Util.pf("Initializing aggregators for %s \n", blockcode);
					for(MyLogType ltype : new MyLogType[] { MyLogType.imp, MyLogType.conversion, MyLogType.pixel, MyLogType.click })
					{
						_lookupMap.get(bcode.toTimeStamp()).put(ltype, new Aggregator(ltype));
					}
				}
			}
		}
	}	
}
