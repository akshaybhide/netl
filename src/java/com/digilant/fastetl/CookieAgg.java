
package com.digilant.fastetl;

import java.io.*;
import java.util.*;

import com.adnetik.shared.FileUtils;
import com.adnetik.shared.LogEntry;
import com.adnetik.shared.Util;
import com.adnetik.shared.WtpId;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.digilant.fastetl.FastUtil.*;

public class CookieAgg
{
	private Map<MyLogType, Map<Integer, SortedMap<WtpId, InfoPack>>> _bigAgg = Util.hashmap();
	FileManager _fileMan;
	InterestManager _intMan;
	public CookieAgg(FileManager f, InterestManager i)
	{
		_fileMan = f;
		_intMan = i;
		for(MyLogType mlt : MyLogType.values())
		{
			Map<Integer, SortedMap<WtpId, InfoPack>> onemap = Util.hashmap();
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
			Util.setdefault(_bigAgg.get(mlt), relid, new TreeMap<WtpId, InfoPack>());

			Scanner sc = new Scanner(new File(onepath));
			while(sc.hasNext())
			{
				WtpId wid = new WtpId(sc.next());
				InfoPack ipack = new InfoPack(sc.nextInt(), sc.nextLong(), sc.nextLong());
				
				// No duplicates!!
				//Util.massert(!_bigAgg.get(mlt).get(relid).containsKey(wid));
				//_bigAgg.get(mlt).get(relid).put(wid, ipack);
				if(_bigAgg.get(mlt).get(relid).containsKey(wid))
				{ _bigAgg.get(mlt).get(relid).get(wid).setLast(ipack.last);
				_bigAgg.get(mlt).get(relid).get(wid).setFirst(ipack.frst);
				_bigAgg.get(mlt).get(relid).get(wid).addCount(ipack.count);
				}
			else
				{ _bigAgg.get(mlt).get(relid).put(wid, ipack);
				}		
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
		Util.setdefault(_bigAgg.get(mltype), relid, new TreeMap<WtpId, InfoPack>());
		
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
				String stagepath = _fileMan.getCookiePath(false, mltype, relid);
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
				List<String> flines = Util.vector();
				String versionfilepath = stagepath + ".version";
				FileUtils.createDirForPath(versionfilepath);
				flines.add(FastUtil.getNowString());
				FileUtils.writeFileLines(flines, versionfilepath);

			}
		}
	}
	void writeToStaging(MyLogType mltype, Integer relid) throws IOException
	{
		//Util.pf("Incrementally write2staging...\n");
		
				Map<WtpId, InfoPack> relmap = _bigAgg.get(mltype).get(relid);
				if(relmap == null)
					return;
				if(relmap.isEmpty())
					{ return; }
				
				// false=staging
				String stagepath = _fileMan.getCookiePath(false, mltype, relid);
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
				List<String> flines = Util.vector();
				String versionfilepath = stagepath + ".version";
				FileUtils.createDirForPath(versionfilepath);
				flines.add(FastUtil.getNowString());
				FileUtils.writeFileLines(flines, versionfilepath);
				_bigAgg.get(mltype).get(relid).clear();

	}
}

