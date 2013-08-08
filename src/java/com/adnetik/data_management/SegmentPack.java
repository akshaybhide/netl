
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

// This class represents a generic package of "segment" data.
// Originally this segment data was from third parties such as Exelate and Bluekai.
// But now we are extending it to represent "first party" data that we generate 
// from our own bidding activities.
// Specific segment data sets should extend this class.
public abstract class SegmentPack<T extends Comparable<T>>
{
	public enum SpecialData  { bkuserid, country }	
	
	TreeMap<SpecialData, String> _specData = Util.treemap();
	
	// SegId --> DayCode
	// Experimenting with hashmap, but it didn't seem to give me much improvement
	
	Map<T, String> _segData = Util.treemap();
	String wtpid;
	
	SegmentPack(String wid)
	{
		wtpid = wid;
	}
	
	public String getWtpId()
	{
		return wtpid;	
	}
	
	SegmentPack(Map.Entry<String, List<String>> myentry)
	{
		this(myentry.getKey());
		
		for(String oneline : myentry.getValue())
		{
			StringTokenizer linetoks = new StringTokenizer(oneline, "\t");
			String wtpid = linetoks.nextToken();
			String daycode = linetoks.nextToken();
			String segstr = linetoks.nextToken();
			String specialstr = (linetoks.hasMoreTokens() ? linetoks.nextToken() : null);
			
			{
				int pos = 0;
				
				while(true)
				{
					int end = segstr.indexOf(',', pos);
					
					String oneseg = (end == -1 ? 
						segstr.substring(pos) :
						segstr.substring(pos, end));
					
					T segid = getSegId(oneseg.trim());
					
					// Integer segid = Integer.valueOf(oneseg.trim());
					
					_segData.put(segid, daycode);
					
					if(end == -1)
						{ break; }
					
					pos = end+1;
				}
			}
			
			// Special data fields
			if(specialstr != null)
			{
				String[] spec_kv = specialstr.split(",");
				for(String kvstr : spec_kv)
				{
					if(kvstr.trim().length() == 0)
						{ continue; }
					
					String[] key_val = kvstr.split("=");
					try { 
						SpecialData sdata = SpecialData.valueOf(key_val[0]);
						_specData.put(sdata, key_val[1]); 
						} catch (Exception ex) { }
				}
			}
		}
	}
	
	public boolean hasSegmentEver(Integer segid)
	{
		return _segData.containsKey(segid);
	}
	
	public Set<T> getAllSegData()
	{
		return _segData.keySet();
	}
	
	public Map<T, String> getSegDataMap()
	{
		return Collections.unmodifiableMap(_segData);	
	}
	
	public abstract T getSegId(String oneseg);	
	
	// Okay, so this should work for MOST data types,
	// but subclasses can override if necessary.
	void integrateNewData(Collection<String> newdatalist, String daycode)
	{
		for(String oneline : newdatalist)
		{
			try {		
				// WTP, SEG, CTRY
				String[] wtp_day_seglist = oneline.split("\t");
				
				// TODO: what about special data?
				Util.massert(wtp_day_seglist.length == 3, "Badly formatted row got past preprocessor");
				
				// Lots of paranoid error checking
				// Check that the WTP is valid
				WtpId wid = WtpId.getOrNull(wtp_day_seglist[0]);
				
				Util.massert(wid != null, "Badly formatted WTP ID %s got past preprocessor", wtp_day_seglist[0]);
				Util.massert(wtpid.equals(wtp_day_seglist[0]), 
					"Associated with user %s found line %s", wtpid, oneline);
				
				TimeUtil.assertValidDayCode(wtp_day_seglist[1]);
				
				
				for(String oneseg : wtp_day_seglist[2].split(","))
				{
					T segid = getSegId(oneseg);
					
					if(_segData.containsKey(segid))
					{
						//Util.pf("Already have segid %s=%s, overwriting with %s\n",
						//	segid, _segData.get(segid), wtp_day_seglist[1]);
						
					}
					
					_segData.put(segid, wtp_day_seglist[1]);
				}
				
				
			} catch (Exception ex) {
				
				Util.pf("Exception due to badly formatted row? : \n%s\n", oneline);
				ex.printStackTrace();
				// errlist.add(oneline);
			}	
		}			
	}
	
	// This will almost always be toString(), but in theory 
	// subclasses may want to override
	public String segId2String(T segid)
	{
		return segid.toString();	
	}
	
	private String getSpecDataStr()
	{
		// Transform map into a string
		String s = _specData.toString();
		s = s.substring(1, s.length()-1);
		return s;
	}
	
	int write(Writer mwrite) throws IOException
	{
		return write(mwrite, "1900-01-01");
	}		
	
	int write(Writer mwrite, String afterdateInc) throws IOException
	{
		int wcount = 0;
		// TreeMap<String, Set<Integer>> revmap = Util.treemap();
		TreeMap<String, LinkedList<T>> revmap = Util.treemap();
		
		for(Map.Entry<T, String> segpair : _segData.entrySet())
		{
			String daycode = segpair.getValue();
			
			// TODO: Okay, this should be a LinkedList<Integer> for performance reasons. 
			// We will get ordering by the fact that _segData is ordered
			// Also, we shouldn't use setdefault here, instead we should do llist = get(daycode),
			// and then if it's null, do an appropriate initialization
			// Util.setdefault(revmap, daycode, new TreeSet<Integer>());
			
			LinkedList<T> segdaylist = revmap.get(daycode);
			
			if(segdaylist == null)
			{
				segdaylist = new LinkedList<T>();
				revmap.put(daycode, segdaylist);
			}
			
			segdaylist.add(segpair.getKey());
			
			// revmap.get(daycode).add(seg);
		}
		
		while(!revmap.isEmpty())
		{
			Map.Entry<String, LinkedList<T>> dayentry = revmap.pollFirstEntry();
			String daycode = dayentry.getKey();
			LinkedList<T> segdaylist = dayentry.getValue();
			
			// Skip data that is before the after-date cutoff
			if(daycode.compareTo(afterdateInc) < 0)
				{ continue; }
			
			// TODO: this should be rewritten to do the string operations "by hand",
			// don't rely on the TreeSet.toString() method
			
			mwrite.write(wtpid);
			mwrite.write("\t");
			mwrite.write(daycode);
			mwrite.write("\t");				
			
			while(!segdaylist.isEmpty())
			{
				T segid = segdaylist.pollFirst();
				
				mwrite.write(segId2String(segid));
				
				if(!segdaylist.isEmpty())
					{ mwrite.write(","); }
			}
			
			// Vague concerns here about writing an extra tab right before the newline
			mwrite.write("\t");
			
			// Write spec-data only on the final row for the user
			mwrite.write(revmap.isEmpty() ? getSpecDataStr() : "");
			mwrite.write("\n");
			
			wcount++;
		}
		
		return wcount;
	}

	
	public abstract static class IntPack extends SegmentPack<Integer>
	{
		public IntPack(String wid)
		{ super(wid); }
		
		public IntPack(Map.Entry<String, List<String>> myentry)
		{ super(myentry); }
		
		public Integer getSegId(String oneseg)
		{	
			return Integer.valueOf(oneseg);
		}
	}	
	
}
