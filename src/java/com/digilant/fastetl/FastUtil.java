package com.digilant.fastetl;

import java.util.*;
import java.util.zip.GZIPInputStream;
import java.io.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.LogEntry;
import com.adnetik.shared.Pair;
import com.adnetik.shared.Util;
import com.adnetik.shared.WtpId;

public class FastUtil 
{
	//public static final String USERVER_NFS_PATH = "/home/armita/mnt/adnetik/adnetik-uservervillage";
	
	//public static final String BASE_PATH = "/home/armita/local/bidder_logs/fastetl";
	
	//public static final String USERVER_NFS_PATH = "/mnt/adnetik/adnetik-uservervillage";
	
	//public static final String BASE_PATH = "/local/bidder_logs/fastetl";
	
	// public static final MyLogType[] LTYPES = new MyLogType[] { LogType.imp, LogType.click, MyLogType.conversion, */MyLogType.pixel };	
	public enum MyLogType { imp, click, conversion, pixel, bid_all, no_bid, no_bid_all};
	
	public static int CompareDates(Calendar cal1, Calendar cal2){
		/*if(cal2.before(cal1)){
			Calendar tmp = cal2;
			cal2= cal1;
			cal1 = tmp;
		}*/
		int cnt = 0;
		while(cal1.before(cal2)){
			cnt++;
			cal1.add(Calendar.DATE, 1);
		}
		return cnt;
	}

	
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
			
			//Util.massert(bcode.toTimeStamp().equals(blockkey), "ERROR, this timestamp is not a STRICT blockkey");
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
			Calendar cal = Util.longDayCode2Cal(timestamp);
			
			// Max should be 60*15, but use 16 for safety
			for(int i = 0; i < 60 *16; i++)
			{
				int m = cal.get(Calendar.MINUTE);
				
				if((m % 15) == 0)
				{
					String daycode = timestamp.split(" ")[0];
					int hr = cal.get(Calendar.HOUR_OF_DAY);
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
		public int getHour(){return hour;}
		public int getQuartet(){return quat;};
		public String getDaycode(){
			return daycode;
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
		void setFirst(long ts){
			frst = (ts < frst ? ts : frst);

		}
		void setLast(long ts){
			last = (ts > last ? ts : last);

		}
		void addCount(int c){
			count+=c;
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

	public static String getNowString() {
		// TODO Auto-generated method stub
		Calendar c = new GregorianCalendar();
		StringBuffer sb = new StringBuffer();
		String month = ((c.get(Calendar.MONTH)+ 1) < 10)?"0"+(c.get(Calendar.MONTH)+ 1):""+(c.get(Calendar.MONTH)+ 1);
		String day = (c.get(Calendar.DATE) < 10)?"0"+c.get(Calendar.DATE):""+c.get(Calendar.DATE);
		sb.append(c.get(Calendar.YEAR));
		sb.append("-");
		sb.append(month);
		sb.append("-");
		sb.append(day);
		sb.append(" ");
		String hour = ((c.get(Calendar.HOUR_OF_DAY)) < 10)?"0"+(c.get(Calendar.HOUR_OF_DAY)):""+(c.get(Calendar.HOUR_OF_DAY));
		String min = (c.get(Calendar.MINUTE) < 10)?"0"+c.get(Calendar.MINUTE):""+c.get(Calendar.MINUTE);
		String sec = (c.get(Calendar.SECOND) < 10)?"0"+c.get(Calendar.SECOND):""+c.get(Calendar.SECOND);
		sb.append(hour);
		sb.append(":");
		sb.append(min);
		sb.append(":");
		sb.append(sec);
		return sb.toString();

	}

}
