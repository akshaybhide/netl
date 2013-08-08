
package com.adnetik.fastetl;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.fastetl.FastUtil.*;

public class CookieAgg
{
	private Map<MyLogType, Map<Integer, /*SortedMap*/HashMap<WtpId, InfoPack>>> _bigAgg = Util.hashmap();
	
	public CookieAgg()
	{
		for(MyLogType mlt : MyLogType.values())
		{
			Map<Integer, /*SortedMap*/HashMap<WtpId, InfoPack>> onemap = Util.hashmap();
			_bigAgg.put(mlt, onemap);
		}
	}
	
	// aggpathset = all the files in "current" directory
	void loadFromSaveData(Set<String> aggpathset) throws IOException
	{
		for(String onepath : aggpathset)
		{
			if(!onepath.endsWith("cookie"))
				{ continue; }

			// smart parsing to get relid and logtype			
			File onefile = new File(onepath);
			int relid = Integer.valueOf(onefile.getParentFile().getName());
			MyLogType mlt = MyLogType.valueOf(onefile.getName().split("\\.")[0]);

			// Make sure it exists
			Util.setdefault(_bigAgg.get(mlt), relid, new /*TreeMap*/HashMap<WtpId, InfoPack>());

			Scanner sc = new Scanner(new File(onepath));
			while(sc.hasNext())
			{
				WtpId wid = new WtpId(sc.next());
				InfoPack ipack = new InfoPack(sc.nextInt(), sc.nextLong(), sc.nextLong());
				
				// No duplicates!!
				Util.massert(!_bigAgg.get(mlt).get(relid).containsKey(wid));
				_bigAgg.get(mlt).get(relid).put(wid, ipack);
			}
			
			sc.close();				
		}		
	}
	
	boolean processLogEntry(MyLogType mltype, LogEntry logent, Set<Integer> interestset)
	{
		// TODO: how to deal correctly with non-set cookie IDs?
		WtpId wid = WtpId.getOrNull(logent.getField("wtp_user_id"));
		if(wid == null)
			{ return false; }
		
		long tstamp = FastUtil.timeStampLogEntry(logent);
		int relid = FastUtil.getRelevantId(logent);	
		
		// Util.pf("Found data for lineitem %d\n", relid);
		// only add if the relevant id is in the interest set
		if(!interestset.contains(relid))
			{ return false; }
		
		// Util.pf("Found data for INTEREST %d\n", relid);
		
		// Make sure the map is present
		Util.setdefault(_bigAgg.get(mltype), relid, new /*TreeMap*/HashMap<WtpId, InfoPack>());
		
		// TODO; this code is replicated elsewhere, and is also very ugly
		if(_bigAgg.get(mltype).get(relid).containsKey(wid))
			{ _bigAgg.get(mltype).get(relid).get(wid).addTimeStamp(tstamp);
			return true;
			}
		else
			{ _bigAgg.get(mltype).get(relid).put(wid, new InfoPack(tstamp));
				return true;
			}		
	}
	
	// Called every 15 minutes.
	void writeToStaging() throws IOException
	{
		Util.pf("Calling write2staging...\n");
		
		for(MyLogType mltype : _bigAgg.keySet())
		{
			for(int relid : _bigAgg.get(mltype).keySet())
			{
				Map<WtpId, InfoPack> relmap = _bigAgg.get(mltype).get(relid);
				if(relmap.isEmpty())
					{ continue; }
				
				// false=staging
				String stagepath = FastUtil.getCookiePath(false, mltype, relid);
				FileUtils.createDirForPath(stagepath);
				
				BufferedWriter bwrite = FileUtils.getWriter(stagepath);
				for(WtpId wid : relmap.keySet())
				{
					InfoPack ipack = relmap.get(wid);
					String dataline = Util.sprintf("%s\t%d\t%d\t%d\n",
						wid.toString(), ipack.count, ipack.frst, ipack.last);
					bwrite.write(dataline);
				}
				bwrite.close();
			}
		}
	}
}

