package com.adnetik.fastetl;

import java.util.*;
import java.util.zip.GZIPInputStream;
import java.io.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.*;

public class FastUtil 
{
	//public static final String USERVER_NFS_PATH = "/home/armita/mnt/adnetik/adnetik-uservervillage";
	
	//public static final String BASE_PATH = "/home/armita/local/bidder_logs/fastetl";

	public static final String USERVER_NFS_PATH = "/mnt/adnetik/adnetik-uservervillage";
	
	public static final String BASE_PATH = "/local/bidder_logs/fastetl";
	
	// public static final MyLogType[] LTYPES = new MyLogType[] { LogType.imp, LogType.click, MyLogType.conversion, */MyLogType.pixel };	
	public enum MyLogType { imp, click, conversion, pixel };
	
	public static void main(String[] args)
	{
		String mkey = "2012-05-15 01:00:00";
		
		for(int i = 0; i < 100000; i++)
		{
			Util.pf("Key %d is %s\n", i, mkey);
			mkey = nextBlock(mkey);
		}
	}

	public static String prevBlock(String calkey)
	{
		return blockDiff(calkey, -15);
	}

	public static String nextBlock(String calkey)
	{
		return blockDiff(calkey, +15);
	}
	public static LogType getLogType(MyLogType mlt)
	{
		return LogType.valueOf(mlt.toString());
	}
	
	public static MyLogType fromStandard(LogType basic)
	{
		return MyLogType.valueOf(basic.toString());	
	}
    public static BufferedReader getGzipReader(String filename) throws IOException
    {
    	
    	InputStream fileStream = new FileInputStream(filename);
    	InputStream gzipStream = new GZIPInputStream(fileStream);
    	return new BufferedReader(new InputStreamReader(gzipStream, BidLogEntry.BID_LOG_CHARSET), 1000000);
    }
	
	public static String blockDiff(String calkey, int diff)
	{
		Calendar cal = Util.longDayCode2Cal(calkey);
		cal.add(Calendar.MINUTE, diff);
		return Util.cal2LongDayCode(cal);		
	}
	



	private static void updateTotalMap(Map<Integer, SortedMap<WtpId, InfoPack>> total, Map<Integer, SortedMap<WtpId, InfoPack>>  tmp){
		
		for(Integer li_id : tmp.keySet()){
			if (total.containsKey(li_id)){
				for(WtpId wtpid : tmp.get(li_id).keySet()){
					if(total.get(li_id).containsKey(wtpid)){
						total.get(li_id).get(wtpid).addTimeStamp(tmp.get(li_id).get(wtpid).GetLast());
					}
					else{
						total.get(li_id).put(wtpid, tmp.get(li_id).get(wtpid));
					}
				}
			}
			else{
				total.put(li_id, tmp.get(li_id));
			}
		}
	}
	
	

	
	// TODO: this is not the way to do it! We are going to end up reading and writing
	// each file 100 times
	/*
	static void flushQuatlyData(String blockkey, Aggregator myagg) throws IOException
	{
		for(Integer relid : myagg.getIdSet())
		{
			String quatpath = getQuatPath(myagg._relType, relid);
			SortedMap<BlockCode, Integer> quatmap = readQuatlyFile(quatpath);
			String quatkey = getQuatKey(blockkey);
			int totalcount = myagg.getTotal(relid);
			quatmap.put(quatkey, totalcount);
			writeQuatlyFile(quatpath, quatmap);
		}
	}
	*/
	
	// TODO: optimize this, so we don't have to read every file every time. 
	// Can probably keep most of these in memory
	static SortedMap<BlockCode, Integer> readQuatlyFile(String quatpath) throws IOException
	{
		if(!(new File(quatpath)).exists())
			{ return Util.treemap(); }
		
		SortedMap<BlockCode, Integer> qmap = Util.treemap();
		BufferedReader bread = Util.getReader(quatpath);	
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String toks[] = oneline.split("\t");	
			BlockCode bcode = new BlockCode(toks[0], Integer.valueOf(toks[1]), Integer.valueOf(toks[2]));

			int count = Integer.valueOf(toks[3]);
			qmap.put(bcode, count);
		}
		
		bread.close();
		return qmap;
	}
	static void writeCookieFile(String filepath, Map<Integer, SortedMap<WtpId, InfoPack>> total)
	{
		FileUtils.createDirForPath(filepath);
		
		/*
		BufferedWriter bwrite = FileUtils.getWriter(filepath);
		for(Integer quatkey : total.keySet())
		{
			String oneline = Util.sprintf("%s\t%d\n", quatkey, quatmap.get(quatkey));
			bwrite.write(oneline);
		}
		bwrite.close();
		*/
		
	}
	static void writeQuatlyFile(String quatpath, SortedMap<String, Integer> quatmap) throws IOException
	{
		FileUtils.createDirForPath(quatpath);
		
		BufferedWriter bwrite = FileUtils.getWriter(quatpath);
		for(String quatkey : quatmap.keySet())
		{
			String oneline = Util.sprintf("%s\t%d\n", quatkey, quatmap.get(quatkey));
			bwrite.write(oneline);
		}
		bwrite.close();
	}
	
	static String getInterestPath(){
		return BASE_PATH ;
	}

	
	static Set<String> getAggPathSet(boolean is_current)
	{
		File basedir = new File(getBaseDir(is_current));
		Set<String> pathset = Util.treeset();
		recAddPaths(pathset, basedir);
		return pathset;
	}
	
	static void recAddPaths(Set<String> pathset, File mydir)
	{
		File[] files = mydir.listFiles();
		if(files == null) return;
		for(File subfile : files)
		{
			if(subfile.isDirectory())
				{ recAddPaths(pathset, subfile);}
			
			pathset.add(subfile.getAbsolutePath());
		}
	}

	static List<String> getRandomOrdered(Collection<String> filelist) throws IOException
	{
		List<String> shuflist = new Vector<String>(filelist);
		Collections.shuffle(shuflist);		
		return shuflist;
	}
	
	
	static List<String> getTimeStampOrdered(Collection<String> filelist) throws IOException
	{
		int lcount = 0;
		SortedSet<Pair<String, String>> myset = Util.treeset(); 	

		List<String> shuflist = new Vector<String>(filelist);
		//Collections.shuffle(shuflist);
		
		for(String onepath : shuflist)	
		{
			BufferedReader bread = Util.getGzipReader(onepath);	
			String firstline = bread.readLine();
			String firststamp = firstline.split("\t")[0];
			myset.add(Pair.build(firststamp, onepath));
			bread.close();
			
			if(++lcount > 300)
				{ break; }
		}
		
		List<String> ordlist = Util.vector();
		for(Pair<String, String> onepair : myset)
		{
			// Util.pf("File %s has start time %s\n", onepair._2, onepair._1);
			ordlist.add(onepair._2);
		}
		
		Util.pf("Ordered %d files by timestamp", ordlist.size());
		
		return ordlist;
	}	
	
	public static List<String> getNfsPixelPaths(String daycode)
	{
		List<String> lpaths = Util.vector();
		// /mnt/adnetik/adnetik-uservervillage/adnexus/userver_log/no_bid/2011-10-24/2011-10-24-23-59-59.EDT.no_bid_v12.adnexus-rtb-ireland_5e37d.log.gz
		
		//String dirPath = getNfsDirPath(excname, logtype, daycode);
		///mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/
		//USERVER_NFS_PATH = "/mnt/adnetik/adnetik-uservervillage";
		
		String dirPath = Util.sprintf("%s/prod/userver_log/pixel/%s/", FastUtil.USERVER_NFS_PATH, daycode);
		Util.pf("Dir path is %s\n", dirPath);
		
		File dirFile = new File(dirPath);
		if(!dirFile.exists())
			{ return null; }
		
		String[] subfiles = dirFile.list();
		
		for(String s : subfiles)
		{
			String fpath = Util.sprintf("%s%s", dirPath, s);
			//Util.pf("\n\tFPath is %s", fpath);	
			lpaths.add(fpath);
		}
		
		return lpaths;
	}
		
	public static String getJunkPath()
	{
		for(int i = 0; i < 100; i++)
		{
			Random r = new Random();	
			long rid = r.nextLong();
			rid = (rid < 0 ? -rid : rid);
			String junkdir = Util.sprintf("%s/junk/junk_%d", BASE_PATH, rid);
			if(!(new File(junkdir)).exists())
				{ return junkdir;}
		}
		
		throw new RuntimeException("Could not find a good junk directory");
	}	

	public static String getStagingDir(MyLogType mlt, int relid)
	{
		return Util.sprintf("%s/staging/%s/%d",
			BASE_PATH, (mlt == MyLogType.pixel ? "pixel" : "icc"), relid);
	}
	
	public static String getCookiePath(boolean is_current, MyLogType mlt, Integer relid)
	{
		return getGenericPath(is_current, mlt, relid, true);	
	}
	
	public static String getQuatlyPath(boolean is_current, MyLogType mlt, Integer relid)
	{
		return getGenericPath(is_current, mlt, relid, false);
	}	
	
	private static String getGenericPath(boolean is_current, MyLogType mlt, Integer relid, boolean is_cookie)
	{
		return Util.sprintf("%s/%s/%s/%d/%s.%s",
			BASE_PATH, 
			(is_current ? "current" : "staging"), 
			(mlt == MyLogType.pixel ? "pixel" : "icc"),
			relid, mlt, (is_cookie ? "cookie" : "quatly"));		
	}
	
	public static String getBaseDir(boolean is_current)
	{
		return Util.sprintf("%s/%s", BASE_PATH, (is_current ? "current" : "staging"));
	}
	
	public static String getCleanListPath(boolean is_current)
	{
		return Util.sprintf("%s/%s/cleanlist.txt", BASE_PATH, (is_current ? "current" : "staging"));
	}
	
	/*
	public static String getCookieStagingPath(MyLogType mltype, int relid)
	{
		return Util.sprintf("%s/%s/%d/cookie.%s", getCookieStagingPath(),
			(mltype == MyLogType.pixel ? "pixel" : "icc"), relid, mltype);
	}
	*/
	
	public static long timeStampLogEntry(LogEntry le)
	{
		String logts = le.getField("date_time");
		Calendar cal = Util.longDayCode2Cal(logts);
		return cal.getTimeInMillis();			
	}

	public static int getRelevantId(LogEntry le)
	{
		String fname = (le instanceof BidLogEntry ? "line_item_id" : "pixel_id");
		return le.getIntField(fname);
	}
	
	static Set<String> getPathsSince(String date, int numdays)
	{
		List<String> dayrange = TimeUtil.getDateRange(/*TimeUtil.getTodayCode()*/ Util.findDayCode(date), numdays);
		Set<String> pathset = Util.treeset();
		
		for(String oneday : dayrange)
			{ pathset.addAll(getPathsForDay(oneday));}


		Util.pf("Read %d paths for daylist %s\n", pathset.size(), dayrange.toString());
		return pathset;
	}
	
	static Set<String> getPathsForDay(String daycode)
	{
		Set<String> pathset = Util.treeset();

		for(MyLogType ltype : MyLogType.values())
		{
			if(ltype == MyLogType.pixel)
			{
				addIfNonNull(pathset, FastUtil.getNfsPixelPaths(daycode));
				
			} else {

				for(ExcName oneexc : ExcName.values())
					{ addIfNonNull(pathset, /*Util.*/getNfsLogPaths(oneexc, LogType.valueOf(ltype.toString()), daycode)); }
			}
		}
		
		return pathset;
	}	
	public static List<String> getNfsLogPaths(ExcName excname, LogType logtype, String daycode)
	{
		List<String> lpaths = Util.vector();
		// /mnt/adnetik/adnetik-uservervillage/adnexus/userver_log/no_bid/2011-10-24/2011-10-24-23-59-59.EDT.no_bid_v12.adnexus-rtb-ireland_5e37d.log.gz

		String dirPath = getNfsDirPath(excname, logtype, daycode);
		
		//Util.pf("Dir path is %s", dirPath);
		
		File dirFile = new File(dirPath);
		if(!dirFile.exists())
			{ return null; }
		
		String[] subfiles = dirFile.list();
		
		for(String s : subfiles)
		{
			String fpath = Util.sprintf("%s%s", dirPath, s);
			//Util.pf("\n\tFPath is %s", fpath);	
			lpaths.add(fpath);
		}
		
		return lpaths;
	}
	

	public static String getNfsDirPath(ExcName excname, LogType logtype, String daycode)
	{
		return Util.sprintf("%s/%s/userver_log/%s/%s/", USERVER_NFS_PATH, excname, logtype, daycode);
	}
		
	private static void addIfNonNull(Collection<String> addto, Collection<String> addfrom)
	{
		if(addfrom != null)
			{ addto.addAll(addfrom);}
	}
	
	public static class BlockCode implements Comparable<BlockCode>
	{
		String daycode;
		int hour;
		int quat;
		
		public BlockCode(String dc, int hr, int qt)
		{
			Util.massert(0 <= hr && hr < 24);
			Util.massert(0 <= qt && qt < 4);
			
			daycode = dc;
			hour = hr;
			quat = qt;
		}
	
		public String toTimeStamp()
		{
			String hrstr = (hour < 10 ? "0" + hour : "" + hour);
			int min  = quat * 15;
			String mnstr = (min < 10 ? "0" + min : "" + min);
			return Util.sprintf("%s %s:%s:00", daycode, hrstr, mnstr); 
		}
		
		public String getQuatKey()
		{
			String hrstr = (hour < 10 ? "0" + hour : "" + hour);
			return Util.sprintf("%s\t%s\t%d", daycode, hrstr, quat);
		}		
		
		// TODO: fix naming convention
		public static BlockCode fromBlockKey(String blockkey)
		{
			String[] date_time = blockkey.split(" ");
			
			String[] hr_mn_sc = date_time[1].split(":");
			
			BlockCode bcode = new BlockCode(date_time[0], 
				Integer.valueOf(hr_mn_sc[0]), Integer.valueOf(hr_mn_sc[1])/15);
			
			Util.massert(bcode.toTimeStamp().equals(blockkey), "ERROR, this timestamp is not a STRICT blockkey");
			return bcode;
		}
		
		public int compareTo(BlockCode other)
		{
			return getQuatKey().compareTo(other.getQuatKey());	
			
		}
		
		public static BlockCode nextBlock(String timestamp)
		{
			return jumpToBlock(timestamp, +1);
		}
		
		public static BlockCode prevBlock(String timestamp)
		{
			return jumpToBlock(timestamp, -1);
		}		
		
		// TODO: we can do something much more efficient here,
		// by maintaining a Treemap<String, BlockCode> 
		private static BlockCode jumpToBlock(String timestamp, int secdiff)
		{
			// Util.pf("Time stamp is %s\n", timestamp);
			Calendar cal = Util.longDayCode2Cal(timestamp);
			
			// Max should be 60*15, but use 16 for safety
			for(int i = 0; i < 60 *16; i++)
			{
				int m = cal.get(Calendar.MINUTE);
				
				if((m % 15) == 0)
				{
					String daycode = timestamp.split(" ")[0];
					int hr = cal.get(Calendar.HOUR);
					int qt = m / 15;
					return new BlockCode(daycode, hr, qt);
				}
				
				cal.add(Calendar.SECOND, secdiff);
			}
			
			throw new RuntimeException("Could not find BlockCode for timestamp " + timestamp);
		}		
		
		
		
		public long toLong()
		{
			StringBuffer sb = new StringBuffer();
			
			for(String s : daycode.split("-"))
				{ sb.append(s); }
			
			sb.append((hour < 10 ? "0"+hour : hour));
			sb.append(quat);
			
			// Util.pf("Timestamp is %s, long is %d\n", toTimeStamp(), Long.valueOf(sb.toString()));
			
			return Long.valueOf(sb.toString());
		}
	}
	
	public static class InfoPack
	{
		long frst;
		long last;
		
		int count;
		
		public InfoPack(long ts)
		{
			frst = ts;
			last = ts;
			count = 1;
		}
		
		InfoPack(int tc, long F, long L)
		{
			count = tc;
			frst = F;
			last = L;
		}
		
		void addTimeStamp(long ts)
		{
			frst = (ts < frst ? ts : frst);
			last = (ts > last ? ts : last);
			count++;
		}
		
		public String toString(){
			
			return Integer.toString(count) + " " + Long.toString(frst) + " " + Long.toString(last);
			
		}
		
		public long GetFrist(){
			return frst;
		}
		
		public long GetLast(){
			return last;
		}
	}	
}
