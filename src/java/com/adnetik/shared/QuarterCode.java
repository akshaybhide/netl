
package com.adnetik.shared;

import java.util.*;
import java.net.*;

public class QuarterCode implements Comparable<QuarterCode>
{
	private static TreeMap<String, QuarterCode> _LOOKUP = null;
	
	private final short _hour;
	private final short _quat; 
	
	static 
	{
		initQuarterLookup();	
	}
	
	public static QuarterCode prevQuarterFromTime(String timeinfo)
	{	
		// Okay, if we hit a boundary exactly, return it. Otherwise, take the last key that's ahead of target
		String relkey = _LOOKUP.containsKey(timeinfo) ? timeinfo : _LOOKUP.headMap(timeinfo).lastKey();
		return _LOOKUP.get(relkey);
	}
	
	// Okay, this is the ONLY place where the LOOKUP map is modified
	// So other methods don't need to be synchronized since they don't make structural modifications to the map
	private static synchronized void initQuarterLookup()
	{
		_LOOKUP = Util.treemap();
		
		for(short hr = 0; hr < 24; hr++)
		{ 
			for(short qt = 0; qt < 4; qt++)
			{
				QuarterCode qcode = new QuarterCode(hr, qt);
				_LOOKUP.put(qcode.toTimeStamp(), qcode);
			}
		}		
	}
	
	public static QuarterCode curQuartCode()
	{
		GregorianCalendar gregcal = new GregorianCalendar();
		String timeinfo = Util.cal2TimeStamp(gregcal);
		return prevQuarterFromTime(timeinfo);
	}
	
	// These are going to be immutable; callers can't create them
	private QuarterCode(short hr, short qt)
	{
		_hour = hr;
		_quat = qt;
	}
		
	public boolean equals(QuarterCode other)
	{
		return _hour == other._hour && _quat == other._quat;	
	}
	
	public String toTimeStamp()
	{ return toTimeStamp(_hour, _quat); }
	
	public static String toTimeStamp(int hr, int qt)
	{
		String hrstr = (hr < 10 ? "0" + hr : "" + hr);
		int min  = qt * 15;
		String mnstr = (min < 10 ? "0" + min : "" + min);
		return Util.sprintf("%s:%s:00", hrstr, mnstr); 		
	}
	
	public short getHour() { return _hour; }
	public short getQuat() { return _quat; }
	
	public int getLongForm()
	{
		return _hour*10 + _quat;	
	}
		
	// Return null if we rollover
	public QuarterCode nextQuarter()
	{
		String tstamp = toTimeStamp();
		Util.massert(this == _LOOKUP.get(tstamp), "Lookup map is inconsistent");
		
		String nextstamp = _LOOKUP.higherKey(tstamp);
		return (nextstamp == null ? null : _LOOKUP.get(nextstamp));
	}
	
	// Return null if we rollover
	public QuarterCode prevQuarter()
	{
		String tstamp = toTimeStamp();
		Util.massert(this == _LOOKUP.get(tstamp), "Lookup map is inconsistent");
		
		String prevstamp = _LOOKUP.lowerKey(tstamp);
		return (prevstamp == null ? null : _LOOKUP.get(prevstamp));
	}	
	
	public static QuarterCode getFirstQuarter()
	{
		return _LOOKUP.firstEntry().getValue();
	}
	
	public static QuarterCode getLastQuarter()
	{
		return _LOOKUP.lastEntry().getValue();
	}
	
	public int compareTo(QuarterCode other)
	{
		if(_hour != other._hour)
			{ return _hour - other._hour; }
		
		return _quat - other._quat;
	}
	
	// Find qcode corresponding to calendar 
	public static QuarterCode nearestQCode(Map<QuarterCode, Integer> qcmap, String timestr)
	{
		int spm = TimeUtil.secondsPastMidnight(timestr);
		QuarterCode bestqc = null;
		int bestdiff = Integer.MAX_VALUE;
		
		for(QuarterCode qcode : qcmap.keySet())
		{
			int mdiff = qcmap.get(qcode)-spm;
			mdiff = (mdiff < 0 ? -mdiff : mdiff);
			
			if(mdiff < bestdiff)
			{
				bestdiff = mdiff;
				bestqc = qcode;
			}
		}
		
		return bestqc;
	}
	
	public static Map<QuarterCode, Integer> getQuarterIntMap()
	{
		SortedMap<QuarterCode, Integer> imap = Util.treemap();
		
		for(short hr = 0; hr < 24; hr++)
		{
			for(short qt = 0; qt < 4; qt++)
			{
				QuarterCode qcode = new QuarterCode(hr, qt);		
				int secrep = TimeUtil.secondsPastMidnight(qcode.toTimeStamp());
				imap.put(qcode, secrep);
			}
		}
		
		return imap;		
	}
	
	public static Map<QuarterCode, Calendar> getQuarterCalMap(String daycode)
	{
		SortedMap<QuarterCode, Calendar> smap = Util.treemap();
		
		for(short hr = 0; hr < 24; hr++)
		{
			for(short qt = 0; qt < 4; qt++)
			{
				QuarterCode qcode = new QuarterCode(hr, qt);	
				String timestamp = daycode + " " + qcode.toTimeStamp();
				Calendar cal = Util.longDayCode2Cal(timestamp);
				smap.put(qcode, cal);
			}
		}
		
		return smap;
	}
	
	public static void main(String[] args)
	{
		Random jr = new Random();
		
		for(int i = 0; i < 1000000; i++)
		{
			GregorianCalendar calA = new GregorianCalendar();
			int offa = jr.nextInt();
			calA.add(Calendar.SECOND, offa);
			String[] date_time = Util.cal2LongDayCode(new GregorianCalendar()).split(" ");		
			QuarterCode qcode = QuarterCode.prevQuarterFromTime(date_time[1]);
			
			Util.massert(qcode != null);
			
			Util.massert(qcode.prevQuarter() == null || qcode.prevQuarter().nextQuarter().equals(qcode));
			Util.massert(qcode.nextQuarter() == null || qcode.nextQuarter().prevQuarter().equals(qcode));
			
		}		
		
		Util.pf("Check passed\n");
	}
}

