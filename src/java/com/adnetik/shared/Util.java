
package com.adnetik.shared;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.util.zip.GZIPInputStream;


import javax.net.ssl.*;
import java.security.cert.X509Certificate;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;

// Do not include Hadoop imports in this file


public class Util
{	
	public static boolean NO_SPRINTF = false;
	
	public static final String RESOURCE_BASE = "/com/adnetik/resources";
	
	public static final long SHIFTME = 1L;	
	
	private static SimpleDateFormat LONG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static SimpleDateFormat NFS_PATH_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
	
	public static final String BIG_PRINT_BAR = "------------------------------------------------------------";
	
	public static final String WTP_ZERO_ID = "00000000-0000-0000-0000-000000000000";
	public static final String WTP_MAXX_ID = WTP_ZERO_ID.replaceAll("0", "F");
	
	
	public static final String USERVER_NFS_PATH = "/mnt/adnetik/adnetik-uservervillage";
	//public static final String HDFS_TEMP_COPY = "/mnt/data/temp_nfs_copy";
	
	public static final String SCRAP_EPS = "/tmp/burfoot/scrap.eps";
	
	public static final String DUMB_SEP = "_____";
	
	// Can add to as necessary
	public enum CountryCode { US, GB, ES, NL, CA, DE, BR, MX, CO };
	
	public enum StandardPass { greenplum };
	
	public enum AdBoardMachine { 
		
		prod("adboard.digilant.com"),
		staging("adboard-staging.digilant.com"),
		next("adboard-next.digilant.com");
		
		private String _hName;
		
		AdBoardMachine(String hostname)
		{
			_hName = hostname;
		}
		
		public String getHostName()
		{
			return _hName;	
		}
	}
	
	public enum ExcName { 
		adbrite, admeld("admeld_mobile"), adnexus, casale, contextweb, dbh, 
		admeta, improvedigital("id", "improve_digital"),  
		facebook,
		pubmatic, 
		liveintent, 
		nexage, openx, rtb("google", "google_video"), rubicon, yahoo, spotx; 
	
		private Set<String> _nameSet; 
		
		ExcName(String... extranames)
		{
			Set<String> nameset = Util.treeset();
			nameset.add(toString());
			
			for(String onename : extranames)
				{ nameset.add(onename); }
			
			_nameSet = Collections.unmodifiableSet(nameset);
		}
	
		public Set<String> getNameSet()
		{
			return _nameSet;	
		} 
	};

	public enum Part3Code { BK, EX };
		
	public enum DataCenter { ireland, virginia, spain, singapore, california };
	
	public enum EtlJob { LocalMode, NewLog2Bin };
	
	public enum LogVersion { v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, UIndexMinVers2 };

	// Need to have "no_bid" in here because DBH uses it
	public enum LogType { no_bid_all, no_bid, bid_pre_filtered, bid_all, imp, click, conversion, activity, UIndexMinType };
	
	public enum ShortDowCode { mon, tue, wed, thr, fri, sat, sun };
	
	public enum AdminEmail {
		// These should align with actual user logins
		burfoot("daniel.burfoot@digilant.com"),
		trev("trevor.blackford@digilant.com"),
		huimin("huimin.ye@digilant.com"),
		sekhark("sekhar.krothapalli@digilant.com"),
		ajaffer("aubrey.jaffer@digilant.com"),
		raj("raj.joshi@digilant.com");
		
		private String _emailAddr;
		
		AdminEmail(String addr)
		{
			_emailAddr = addr;	
		}
		
		public String getEmailAddr()
		{
			return _emailAddr;	
		}
	}

	
	public static final Set<String> BROWSER_CODES = Util.treeset();
	public static final Set<String> OS_CODES = Util.treeset();
	
	static {
		BROWSER_CODES.add("Chrome");
		BROWSER_CODES.add("Firefox");
		BROWSER_CODES.add("Safari");
		BROWSER_CODES.add("Opera");
		BROWSER_CODES.add("MSIE");
		BROWSER_CODES.add("Other");
		
		OS_CODES.add("Windows");
		OS_CODES.add("Mac");
		OS_CODES.add("Other");
		OS_CODES.add("Linux");		
	}
	
	// public static final String S3N_BUCKET_PREF = "s3n://adnetik-uservervillage/";
	
	private static Set<String> _TOPLEV = null;
	
	// Length of a day in milliseconds
	public static final Long DAY_MILLI = 24*60*60*1000L;
	
	@SuppressWarnings("unchecked")
	public static <T> T cast(Object o)
	{
		return (T) o;	
	}
	
	public static <A> List<A> listify(A... items)
	{
		Vector<A> mylist = vector();
		
		for(A oneitem : items)
			{ mylist.add(oneitem); }
		
		return mylist;
	}
	
	public static <A> Vector<A> vector()
	{
		return new Vector<A>();
	}
	
	public static <A> TreeSet<A> treeset()
	{
		return new TreeSet<A>();
	}	
	
	public static <A,B> TreeMap<A,B> treemap()
	{
		return new TreeMap<A,B>();
	}	
	
	public static <A,B> HashMap<A,B> hashmap()
	{
		return new HashMap<A,B>();
	}		
	
	public static <A,B> LinkedHashMap<A,B> linkedhashmap()
	{
		return new LinkedHashMap<A,B>();
	}			
	
	// Type safe map get method. 
	public static <A,B> B safeget(Map<A, B> mymap, A mykey)
	{
		return mymap.get(mykey);	
	}
	
	public static <A,B> java.util.concurrent.ConcurrentHashMap<A,B> conchashmap()
	{
		return new java.util.concurrent.ConcurrentHashMap<A,B>();
	}			
	
	public static <A> LinkedList<A> linkedlist()
	{
		return new LinkedList<A>();
	}		
	
	
	public static void massert(boolean x)
	{
		massert(x, "Generic assertion");	
		
	}
	
	public static void massert(boolean x, String mssg, Object... vargs)
	{
		if(!x)
		{
			String em = Util.sprintf(mssg, vargs);
			//System.out.printf("\n" + mssg + "\n", vargs);
			throw new RuntimeException(em);
		}
		
	}
	
	public static void massertEq(long a, long b)
	{
		massert(a == b, "Values not equal a=%d b=%d", a, b);
		
	}
	
	public static boolean unixsort(String filepath, String clargs) throws IOException
	{
		String temppath = Util.sprintf("%s__sort", filepath);
		return unixsort(filepath, temppath, clargs);		
	}
	
	public static boolean unixsort(String filepath, String temppath, String clargs) throws IOException
	{
		double startup = Util.curtime();
		String s = null;
		boolean hadErr = false;
		String sortcall = Util.sprintf("sort %s %s", clargs, filepath);
		
		FileWriter reswrite = new FileWriter(temppath);

		Process p = Runtime.getRuntime().exec(sortcall);
		
		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
		BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		
		while ((s = stdInput.readLine()) != null) {
			reswrite.write(s);
			reswrite.write("\n");
		}
		
		stdInput.close();
		reswrite.close();
		
		while ((s = stdError.readLine()) != null) 
		{
			System.err.println(s);
			hadErr = true;
		}
		
		if(!hadErr)
		{
			Util.pf("Sort operation successful for file %s, took %.03f secs, renaming... ", filepath, (Util.curtime()-startup)/1000);	
			File orig = new File(filepath);
			File temp = new File(temppath);
			orig.delete();
			temp.renameTo(orig);
			Util.pf(" done\n");	
		
		} else {
			Util.pf("Errors detected on sort operation\n");	
		}
		
		stdError.close();
		
		return hadErr;
	}
	
	public static boolean syscall(String comm, List<String> inlist, List<String> outlist, List<String> errlist)
	throws IOException
	{
		String s = null;
		boolean hadErr = false;
		
		// run the Unix "ps -ef" command
		// using the Runtime exec method:
		Process p = Runtime.getRuntime().exec(comm);
		
		if(!inlist.isEmpty())
		{
			PrintWriter pwrite = new PrintWriter(p.getOutputStream());
			for(String inline : inlist)
			{
				pwrite.write(inline);
				pwrite.write("\n");
			}
			pwrite.close();
		}
		
		
		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
		
		BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		
		// read the output from the command
		//System.out.println("Here is the standard output of the command:\n");
		while ((s = stdInput.readLine()) != null) {
			//System.out.println(s);
			outlist.add(s);
		}
		
		// read any errors from the attempted command
		// System.out.println("Here is the standard error of the command (if any):\n");
		while ((s = stdError.readLine()) != null) {
			errlist.add(s);
			hadErr = true;
		}
		
		return hadErr;		
	}
	
	
	public static boolean syscall(String comm, List<String> outList, List<String> errList) throws IOException
	{
		return syscall(comm, new Vector<String>(), outList, errList);
	}
	
	public static List<String> syscall(String comm) throws IOException
	{
		List<String> outList = Util.vector();
		List<String> errList = Util.vector();
		
		syscall(comm, outList, errList);
		
		return outList;
	}
	
	public static int getMedian(SortedMap<Integer, Integer> metacount)
	{
		int tot = 0;
		
		for(Integer onecount : metacount.values())
			{ tot += onecount; }
		
		int cumsum = 0;
		
		for(Integer countkey : metacount.keySet())
		{
			cumsum += metacount.get(countkey);
			
			if(cumsum >= tot/2)
				{ return countkey; }
		}
		
		throw new RuntimeException("Should never happen");		
	}
	
	// Uses the AdminEmail package
	// You better have 
	public static AdminEmail getUserEmail()
	{
		String username = getUserName();
		
		try { return AdminEmail.valueOf(username); }
		catch (IllegalArgumentException illex) { return null; }	
		
	}

	public static String getUserName()
	{
		return System.getProperty("user.name"); 		
	}

	
	public static <T> void incHitMap(Map<T, Integer> hitMap, T hit)
	{
		incHitMap(hitMap, hit, 1);
	}
	
	public static <T> void incHitMap(Map<T, Integer> hitMap, T hit, int toInc)
	{
		int c = hitMap.containsKey(hit) ? hitMap.get(hit) : 0;

		hitMap.put(hit, c+toInc);
	}	
	
	public static <T> SortedMap<Double, T> invHitMap(Map<T, Integer> hitMap)
	{	
		
		SortedMap<Double, T> invMap = new TreeMap<Double, T>(Collections.reverseOrder());

		for(T key : hitMap.keySet())
		{
			double v = hitMap.get(key);
			
			while(invMap.containsKey(v))
				{ v -= .0000001; }
			
			
			invMap.put(v, key);
		}
		
		return invMap;
	}
	
	
	
	public static Pair<String, String> splitOnFirst(String bigstr, char delim)
	{
		return splitOnFirst(bigstr, ""+delim);
	}
	
	public static Pair<String, String> splitOnFirst(String bigstr, String delim)
	{
		int i = bigstr.indexOf(delim);
		
		Util.massert( i != -1, "Delimeter %s not found in string ", delim);		
		
		String a = bigstr.substring(0, i);
		String b = bigstr.substring(i+1);
		
		return Pair.build(a, b);
	}
	
	// Danger, Will Robinson: this should not be used in performance intensive areas
	public static String sprintf(String format, Object... varargs)
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintWriter pw = new PrintWriter(baos);
		pw.printf(format, varargs);
		pw.close();
		return baos.toString();
	}
	
	public static Set<String> getTopLevelSet()
	{
		if(_TOPLEV == null)
		{
			_TOPLEV = Util.treeset();	
			_TOPLEV.add("com");
			_TOPLEV.add("edu");
			_TOPLEV.add("co");
			_TOPLEV.add("net");
			_TOPLEV.add("in");
			_TOPLEV.add("es");
			_TOPLEV.add("org");
			_TOPLEV.add("tv");
			_TOPLEV.add("ru");	
			_TOPLEV.add("fm");
			_TOPLEV.add("fr");
			_TOPLEV.add("se");
			_TOPLEV.add("it");
			_TOPLEV.add("nu");	
			_TOPLEV.add("cx");	
			_TOPLEV.add("eu");				
			_TOPLEV.add("be");				
			_TOPLEV.add("vg");				
			_TOPLEV.add("jp");
			_TOPLEV.add("st");			
			_TOPLEV.add("de");	
			_TOPLEV.add("ca");
			_TOPLEV.add("cl");			
			_TOPLEV.add("us");			
			_TOPLEV.add("info");		
			_TOPLEV.add("biz");			
			_TOPLEV.add("cc");
			_TOPLEV.add("cn");			
		}
		
		return _TOPLEV;		
	}

	public static String getDomainFromUrl(String url)
	{
		String[] domtop = getDomTopFromUrl(url);
		return (domtop == null ? null : domtop[0]);
	}

	public static String getTopLevelFromUrl(String url)
	{
		String[] domtop = getDomTopFromUrl(url);
		return (domtop == null ? null : domtop[1]);
	}
	
	// Get domain and top-level from the url
	public static String[] getDomTopFromUrl(String url)
	{
		String[] toks = url.split("/");	
		
		for(String tok : toks)
		{
			if(tok.indexOf(".") > -1)
			{
				String[] subtoks = tok.split("\\.");
				
				// NB - strictly >0 because we don't run for n=0
				for(int n = subtoks.length-1; n > 0; n--)
				{
					if(getTopLevelSet().contains(subtoks[n]))
					{
						return new String[] { subtoks[n-1], subtoks[n] };
					}
				}
			}
		}		
		return null;
	}	
	
	/*
	public static Path[] loadPathListFromFile(String filePath, String pref) throws IOException
	{
		List<String> files = FileUtils.readFileLines(filePath);
		Path[] paths = new Path[files.size()];
		
		for(int i = 0; i < files.size(); i++)
		{
			paths[i] = new Path(pref + files.get(i));
		}
		
		return paths;		
	}
	


	public static Path[] readPathsFromMani(String manifestFile, String pref)
	{
		List<String> buckets = FileUtils.readFileLinesE(manifestFile);
		Path[] paths = new Path[buckets.size()];
		
		for(int i = 0; i < buckets.size(); i++)
		{
			String fullpath = Util.sprintf("%s%s", pref, buckets.get(i));
			//System.out.printf("\nFull path is %s", fullpath);
			paths[i] = new Path(fullpath);
		}
		
		return paths;
	}
	
	public static Path[] removeNonExistingPaths(FileSystem fsys, Path[] paths) throws IOException
	{
		List<Path> existlist = Util.vector();
		
		for(Path p : paths)
		{
			if(fsys.exists(p))
				{ existlist.add(p); }
		}
		
		return existlist.toArray(new Path[] {} );
	}
	*/
	
	
	// Map of Log Type to number of days we save them
	// so no_bid is saved for 5 days, bid_all
	public static Map<String, Integer> logTypeSaveMap()
	{
		Map<String, Integer> smap = Util.treemap();
		
		for(Map.Entry<LogType, Integer> lentry : logTypeOnlySaveMap().entrySet())
			{ smap.put(lentry.getKey().toString(), lentry.getValue()); }
		
		// Add pixel information
		smap.put("pixel", 90);
		
		return smap;
	}
	
	public static Map<LogType, Integer> logTypeOnlySaveMap()
	{
		Map<LogType, Integer> smap = Util.treemap();
		
		smap.put(LogType.bid_pre_filtered, 5);
		smap.put(LogType.no_bid_all, 7);
		smap.put(LogType.bid_all, 14);
		smap.put(LogType.imp, 90);
		
		// Basically, never delete these
		smap.put(LogType.click, 1000);
		smap.put(LogType.conversion, 1000);
		
		return smap;		
	}
	
	
	public static int numSaveDays(LogType logtype)
	{
		return numSaveDays(logtype.toString());
	}
	
	public static int numSaveDays(String logtype)
	{
		return logTypeSaveMap().get(logtype);
	}
		
	public static LogType[] getMiniLogTypes()
	{
		return new LogType[] { LogType.conversion, LogType.click, LogType.imp };
	}
	
	public static LogType[] getBigLogTypes()
	{
		return new LogType[] { LogType.bid_all, LogType.no_bid_all };
	}
	
	public static boolean isBig(LogType lt)
	{
		return lt == LogType.bid_all || lt == LogType.no_bid_all || lt == LogType.bid_pre_filtered;
	}

	// TODO: finish migrating all these functions into TimeUtil	
	public static String cal2LongDayCode(Calendar cal)
	{
		return TimeUtil.cal2LongDayCode(cal);
	}	

	public static String cal2TimeStamp(Calendar cal)
	{
		return TimeUtil.cal2TimeStamp(cal);
	}
	
	private static Calendar tstamp2Cal(String timestamp, SimpleDateFormat dform)
	{
		try {
			synchronized (dform) 
			{
				Date d = dform.parse(timestamp);
				Calendar cal = new GregorianCalendar();
				cal.setTime(d);
				return cal;
			}
		} catch(java.text.ParseException pex) {
			throw new RuntimeException(pex);
		}		
	}
	
	public static Calendar longDayCode2Cal(String timestamp) 
	{
		return tstamp2Cal(timestamp, LONG_DATE_FORMAT);
	}	
	
	private static Map<String, ExcName> _EXCLOOKUP;
	
	public static ExcName excLookup(String s)
	{
		if(_EXCLOOKUP == null)
		{
			_EXCLOOKUP = Util.treemap();	
			
			for(ExcName exc : ExcName.values())
			{
				for(String onename : exc.getNameSet())
				{ Util.putNoDup(_EXCLOOKUP, onename, exc); }
			}
		}
		
		if(!_EXCLOOKUP.containsKey(s))
		{
			throw new RuntimeException("Exchange code not found: " + s);	
		}
		
		return _EXCLOOKUP.get(s);
	}
	
	public static String varjoin(String glue, Object... varargs)
	{
		return join(varargs, glue);
	}
	
	@SuppressWarnings("unchecked")
	public static String join(Collection data, String glue)
	{
		return join(data.toArray(new Object[] {} ), glue);
	}
	
	public static String join(Object[] data, String glue)
	{
		return joinSub(data, glue, 0);
	}
	
	public static String joinButFirst(Object[] data, String glue)
	{
		return joinSub(data, glue, 1);
	}
	
	private static String joinSub(Object[] data, String glue, int offset)
	{
		StringBuffer sb = new StringBuffer();
		
		for(int i = offset; i < data.length; i++)
		{
			sb.append(data[i].toString());
			
			if(i < data.length-1)
				sb.append(glue);
		}
		return sb.toString();		
	}
	
	public static void pf(String format, Object... vargs)
	{
		System.out.printf(format, vargs);	
	}
	
	public static ArgMap getClArgMap(String[] args)
	{
		ArgMap amap = new ArgMap();
		amap.putClArgs(args);
		return amap;
	}
	
	public static class ArgMap extends TreeMap<String, String>
	{
		public ArgMap()
		{
			super();	
		}
		
		public void putClArgs(String[] args)
		{
			for(String onearg : args)
			{
				if(onearg.split("=").length == 2)
				{
					String[] keyval = onearg.split("=");
					put(keyval[0], keyval[1]);
				}
			}			
		}
			
		public double getDouble(String code, double def)
		{
			return containsKey(code) ? Double.valueOf(get(code)) : def;
		}		
		
		public boolean getBoolean(String code, boolean def)
		{
			return containsKey(code) ? Boolean.valueOf(get(code)) : def;
		}
		
		public int getInt(String code, int def)
		{
			return containsKey(code) ? Integer.valueOf(get(code)) : def;
		}
		
		public String getDayCode(String code, String def)
		{
			String dc = getString(code, def);
			Util.massert(TimeUtil.checkDayCode(dc), "Invalid day code %s", dc);
			return dc;
		}
		
		public String getString(String code, String def)
		{
			return containsKey(code) ? get(code) : def;	
		}
		
	}
	
	// TODO: remove this, replace all calling code with ArgMap
	public static void putClArgs(String[] args, Map<String, String> putIn)
	{
		for(String onearg : args)
		{
			if(onearg.split("=").length == 2)
			{
				String[] keyval = onearg.split("=");
				putIn.put(keyval[0], keyval[1]);
			}
		}
	}
	
	public static boolean hasNonBasicAscii(String s)
	{
		for(int i = 0; i < s.length(); i++)
		{
			int x = (int) s.charAt(i);
			
			if(x >= 128)
				{ return true; }
		}
		return false;	
	}
	
	public static String basicAsciiVersion(String s)
	{
		StringBuffer sb = new StringBuffer();
		
		for(int i = 0; i < s.length(); i++)
		{
			int x = (int) s.charAt(i);
			
			if(x >= 128)
				{ continue; }			
			
			sb.append(s.charAt(i));
		}
		
		return sb.toString();
	}	
	
	private static Map<String, Double> _EXCH_FACTOR;

	// TODO: should this go as a derived field in BidLogEntry...?	
	public static double getWinnerPriceCpm(BidLogEntry ble)
	{
		return convertPrice(ble.getField(LogField.ad_exchange), ble.getDblField(LogField.winner_price));
	}
	
	public static double convertPrice(String exchange, double baseval)
	{
		if(_EXCH_FACTOR == null)
		{
			_EXCH_FACTOR = Util.treemap();
			_EXCH_FACTOR.put("adbrite", 1.0);
			_EXCH_FACTOR.put("admeld", 1.0);
			_EXCH_FACTOR.put("admeta", 1.0);
			_EXCH_FACTOR.put("dbh", 1.0);
			_EXCH_FACTOR.put("adnexus", 1.0);
			_EXCH_FACTOR.put("casale", 1.0);
			_EXCH_FACTOR.put("contextweb", 1.0);
			_EXCH_FACTOR.put("id", 1.0);
			_EXCH_FACTOR.put("facebook", 1.0);
			_EXCH_FACTOR.put("improve_digital", 1.0); // =id
			_EXCH_FACTOR.put("openx", .001);
			_EXCH_FACTOR.put("rtb", .001);
			_EXCH_FACTOR.put("google", .001); // google=rtb
			_EXCH_FACTOR.put("rubicon", 1.0);
			_EXCH_FACTOR.put("yahoo", 1.0);
		}
	
		if(!_EXCH_FACTOR.containsKey(exchange))
		{
			throw new RuntimeException("Unknown exchange: " + exchange);	
			
		}
		
		return _EXCH_FACTOR.get(exchange) * baseval;
	}
	
	public static ExcName getExchange(String path){
		String tmp = path.substring(USERVER_NFS_PATH.length()+1);
		return ExcName.valueOf(tmp.substring(0, tmp.indexOf('/')));
	}
	
	
	public static String getNfsDirPath(ExcName excname, LogType logtype, String daycode)
	{
		return Util.sprintf("%s/%s/userver_log/%s/%s/", USERVER_NFS_PATH, excname, logtype, daycode);
	}
	
	public static LogType logTypeFromNfsPath(String filepath)
	{
		SortedSet<LogType> checkset = Util.treeset();
		
		for(LogType lt : LogType.values())
		{
			if(filepath.indexOf(lt.toString()) > -1)
				{ checkset.add(lt); }
		}
		
		Util.massertEq(checkset.size(), 1);
		return checkset.first();
		
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
		
		// Don't believe we will ever get null here (as opposed to zero-length array)
		// under normal conditions. But potentially a glitch with NFS could cause this.
		String[] subfiles = dirFile.list();
		if(subfiles == null)
			{ return null; }
		
		for(String s : subfiles)
		{
			String fpath = Util.sprintf("%s%s", dirPath, s);
			//Util.pf("\n\tFPath is %s", fpath);	
			lpaths.add(fpath);
		}
		
		return lpaths;
	}
	
	public static Map<String, Calendar> getNfsLogPathCalMap(ExcName excname, LogType logtype, String daycode)
	{
		List<String> pathlist = getNfsLogPaths(excname, logtype, daycode);
		if(pathlist == null)
			{ return null; }
		
		Map<String, Calendar> calmap = Util.treemap();
		for(String onepath : pathlist)
		{
			calmap.put(onepath, TimeUtil.calFromNfsPath(onepath));
		}
		return calmap;
	}
	
	public static String getNfsPixelDir(String daycode)
	{
		return Util.sprintf("/mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/%s", daycode);
	}
	
	public static List<String> getNfsPixelLogPaths(String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);
		
		List<String> pathlist = Util.vector();

		String dirPath = getNfsPixelDir(daycode);
		
		File dirfile = new File(dirPath);
		if(!dirfile.exists())
			{ return null; }
		
		for(String s : dirfile.list())
		{
			String fpath = Util.sprintf("%s/%s", dirPath, s);
			pathlist.add(fpath);
		}

		return pathlist;		
	}
	
	public static Map<String, Long> getNfsPathSizeMap(ExcName excname, LogType logtype, String daycode)
	{
		Map<String, Long> sizemap = Util.treemap();
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
			
			long len = (new File(fpath)).length();
			
			sizemap.put(fpath, len);
			//Util.pf("\n\tFPath is %s, total is %d", fpath, len);		
		
		}
		
		return sizemap;
	}
		
	// Shorthand for System.currentTimeMillis
	public static long curtime()
	{
		return System.currentTimeMillis();		
	}
	
	// Attempt to find the log type and version info from the path
	@Deprecated
	public static Object[] logTypeVersionFromPath(String path)
	{
		for(LogType ltype : LogType.values())
		{
			for(LogVersion lvers : LogVersion.values())
			{
				if(path.indexOf(ltype + "_" + lvers) != -1)
					{ return new Object[] { ltype, lvers };	 }
			}
		}
		
		return null;		
	}
	
	public static DataCenter dataCenterFromPath(String path)
	{
		try { return (new PathInfo(path)).pCenter; }
		catch (Exception ex) { return null; }
	}	
	
	public static String lastToken(String full, String spliton)
	{
		String[] toks = full.split(spliton);
		return toks[toks.length-1];
	}
	
	// These are the exchanges that are big enough
	// that they should not have days without any impressions
	public static ExcName[] bigExchanges()
	{
		return new ExcName[] {
			ExcName.admeld,
			ExcName.adnexus,
			ExcName.casale,
			ExcName.contextweb,
			ExcName.improvedigital,
			ExcName.openx,
			ExcName.rtb,
			ExcName.rubicon
		};
	}
	
	public static void showMemoryInfo()
	{
		showMemoryInfo(new SimpleMail("GIMP"));	
	}
	
	public static void showMemoryInfo(SimpleMail smail)
	{
		Runtime rt = Runtime.getRuntime();
		
		smail.pf("Used memory (bytes): \t%s\n",
			commify(rt.totalMemory() - rt.freeMemory()));
		
		smail.pf("Free memory (bytes): \t%s\n", commify(rt.freeMemory()));
		
		/* This will return Long.MAX_VALUE if there is no preset limit */
		long maxMemory = rt.maxMemory();
		smail.pf("Maximum memory (bytes): \t%s\n", (maxMemory == Long.MAX_VALUE ? "no limit" : commify(maxMemory)));

		/* Total memory currently in use by the JVM */
		smail.pf("Total memory (bytes) : \t%s\n", commify(rt.totalMemory()));
		
        }
        
        public static String commify(long number)
        {
        	StringBuffer sb = new StringBuffer();
        	
        	long val = number;
        	int cc = 0;
        	
        	while(val > 0)
        	{
        		sb.insert(0 , val % 10);
        		
        		val /= 10;
        		cc++;
        		
        		if(cc == 3 && val > 0)
        		{
        			sb.insert(0, ",");
        			cc = 0;
        		}
        	}
        	return sb.toString();
        	
        }
        
        public static BufferedReader getReader(String filename, String encoding) throws IOException
        {
        	InputStream fileStream = new FileInputStream(filename);
        	return new BufferedReader(new InputStreamReader(fileStream, encoding));
        }        
       
        public static BufferedReader getReader(String filename) throws IOException
        {
        	return getReader(filename, BidLogEntry.BID_LOG_CHARSET);
        }                
        
        public static BufferedReader getGzipReader(String filename, String encoding) throws IOException
        {
        	InputStream fileStream = new FileInputStream(filename);
        	InputStream gzipStream = new GZIPInputStream(fileStream);
        	return new BufferedReader(new InputStreamReader(gzipStream, encoding));
        }
               
        public static BufferedReader getGzipReader(String filename) throws IOException
        {
        	return getGzipReader(filename,  BidLogEntry.BID_LOG_CHARSET);
        }



        public static Scanner getGzipScanner(String filename, String encoding) throws IOException
        {
        	InputStream fileStream = new FileInputStream(filename);
        	InputStream gzipStream = new GZIPInputStream(fileStream);
        	return new Scanner(gzipStream, encoding);
        }
        

        public static Scanner getGzipScanner(String filename) throws IOException
        {
        	return getGzipScanner(filename, BidLogEntry.BID_LOG_CHARSET);
        }
        
        public static class Syscall
        {
        	
        	String commline;
        	
        	public List<String> outlist = Util.vector();
        	public List<String> errlist = Util.vector();
        	
        	public Syscall(String cline)
        	{
        		commline = cline;
        	}
        	
        	public void exec(boolean doPrint) throws IOException
        	{
        		syscall(commline, outlist, errlist);	
        		
        		for(String oneout : outlist)
        		{
        			Util.pf("%s", oneout);	
        		}
        		
        		for(String oneerr : errlist)
        		{
        			Util.pf("%s", oneerr);	
        		}
        	}
        
        	public void exec() throws IOException
        	{
        		exec(true);	
        	}
        	
        }
        
        public static String padstr(String s, int fullsize)
        {
        	if(s.length() >= fullsize)
        		{ return s; }
        	
        	StringBuffer sb = new StringBuffer();
        	sb.append(s);
        	
        	while(sb.length() < fullsize)
        		{ sb.append(" "); }
        	
        	return sb.toString();
        	
        }
        
        public static String padLeadingZeros(String p, int numDigits)
        {
        	StringBuilder sb = new StringBuilder();
        	
        	sb.append(p);
        	
        	while(sb.length() < numDigits)
        	{
        		sb.insert(0, "0");	
        	}

        	return sb.toString();        	
        	
        }
        
        public static String padLeadingZeros(int val, int numDigits)
        {
        	return padLeadingZeros(""+val, numDigits);
        } 
        
        public static long ip2long(String ip)
        {
        	String[] toks = ip.split("\\.");
        	
        	if(toks.length != 4)
        		{ throw new IllegalArgumentException("Bad IP : " + ip); }
        	
        	long t = 0;
        	long f = 256*256*256;
        	
		for(String onequad : toks)
		{
			t += f * Integer.valueOf(onequad);
			f /= 256;
		}
		
		return t;
        }
      
        
        public static String long2ip(long ipval)
        {
        	long n = ipval;
        	StringBuilder sb = new StringBuilder();
        	
        	for(long d = 256*256*256; d > 0; d /= 256)
        	{
        		long m = n / d;
        		n %= d;
        		
        		sb.append(m);
        		sb.append(d > 1 ? "." : "");
      
        	}
        	
        	return sb.toString();
        }       
        
        // Python style setdefault
        public static <A, B> void setdefault(Map<A, B> tmap, A key, B defval)
        {
        	if(!tmap.containsKey(key))
        		{ tmap.put(key, defval); }
        }
        
        public static <A> void putNoDup(Set<A> myset, A targ)
        {
        	Util.massert(!myset.contains(targ), "Target %s already present in set", targ);
        	myset.add(targ);
        }
        
        public static <A, B> void putNoDup(Map<A,B> mymap, A key, B val)
        {
        	Util.massert(!mymap.containsKey(key), "Key %s already present in set", key);
        	mymap.put(key, val);
        } 
        
        
        public static <A> SortedMap<Integer, Integer> metaCountMap(Map<A, Integer> countmap)
        {
        	SortedMap<Integer, Integer> metacount = Util.treemap();
        	
		for(Integer count : countmap.values())
		{
			Util.setdefault(metacount, count, 0);
			metacount.put(count, metacount.get(count)+1);			
		}
		return metacount;
        }
        
        public static String findDayCode(String targ)
        {
        	return RegexpUtil.findDayCode(targ);
        }

        // TODO: take this out
        public static LogVersion targetVersionFromDayCode(String daycode)
        {
        	if(daycode.compareTo("2012-02-03") < 0)
        		{ return LogVersion.v13; }
        	
        	return LogVersion.v14;
        }
        
        public static LogVersion targetVersionFromPath(String path)
        {
        	return targetVersionFromDayCode(findDayCode(path));
        }
        
        public static LogVersion fileVersionFromPath(String path)
        {
        	List<LogVersion> vlist = Util.vector();
        	
        	for(LogVersion lver : LogVersion.values())
        	{
        		if(path.indexOf(lver.toString()) > -1)
        			{ vlist.add(lver); }
        	}
        	
        	if(vlist.size() == 1)
        		{ return vlist.get(0); }
        	
        	if(vlist.isEmpty())
        		{ throw new RuntimeException("No version code found in path: " + path); }
        	
        	throw new RuntimeException("Multiple version codes found in path: " + path); 
        	
        }
       
 	public static int dayCode2Int(String daycode)
	{
		return Integer.valueOf(Util.join(daycode.split("-"), ""));
	}       
	
        
	private static final Pattern _WTP_PATTERN = Pattern.compile("[0-9a-f]{8}+-[0-9a-f]{4}+-[0-9a-f]{4}+-[0-9a-f]{4}+-[0-9a-f]{12}+");
	
	public static boolean checkWtp(String astr)
	{
		synchronized ( _WTP_PATTERN )
		{ return _WTP_PATTERN.matcher(astr).matches(); }
	}
	
	public static void disableCertificateValidation() {
		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { 
			new X509TrustManager() {
				public X509Certificate[] getAcceptedIssuers() { 
					return new X509Certificate[0]; 
				}
				public void checkClientTrusted(X509Certificate[] certs, String authType) {}
				public void checkServerTrusted(X509Certificate[] certs, String authType) {}
		}};
		
		// Ignore differences between given hostname and certificate hostname
		HostnameVerifier hv = new HostnameVerifier() {
			public boolean verify(String hostname, SSLSession session) { return true; }
		};
		
		// Install the all-trusting trust manager
		try {
			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			HttpsURLConnection.setDefaultHostnameVerifier(hv);
 		 } catch (Exception e) {}
  	}

	
	public static List<String> httpDownload(String urlToRead) throws IOException
	{
		disableCertificateValidation();
				
		List<String> hlist = Util.vector();
		
		URL url = new URL(urlToRead);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		
		/*
		if(validateHost)
		{
			HostnameVerifier allHostsValid = new HostnameVerifier() {
				public boolean verify(String hostname, SSLSession session) {
					return true;
				}
			};		
			
			
			conn.setDefaultHostnameVerifier(allHostsValid);
		}
		*/
		
		java.nio.charset.Charset myutf8 = java.nio.charset.Charset.forName("UTF-8");
		
		conn.setRequestMethod("GET");
		BufferedReader bread = new BufferedReader(new InputStreamReader(conn.getInputStream(), myutf8));
		
		for(String line = bread.readLine(); line != null; line = bread.readLine())
			{ hlist.add(line); }
		
		bread.close();
		
		return hlist;
	}
	
	public static boolean checkOkay(String promptMssg)
	{
		System.out.printf("\n%s ? [yes/NO] ", promptMssg);			
		
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		sc.close();
		
		return "yes".equals(input.trim());
	}
	
	public static String  promptUser(String promptMssg)
	{
		System.out.printf("%s ", promptMssg);			
		
		Scanner sc = new Scanner(System.in);
		String input = sc.nextLine();
		
		return input.trim();
	}	
	
	
	public static boolean getBit(long a, int bitid)
	{
		long test = (a & (SHIFTME << bitid));
		return (test != 0);
	}
	
	public static long setBit(long a, int bitid)
	{
		a |= (SHIFTME << bitid);
		return a;
	}	
	
	public static byte setBit(byte a, int bitid)
	{
		a |= (SHIFTME << bitid);
		return a;
	}	

	public static long bytes2long(byte[] data)
	{
		Util.massert(data.length == 8);
		
		long a = 0;
		
		for(int i = 0; i < 64; i++)
		{
			boolean set = getBit(data[i/8], i%8);			
			if(set)
				{ a = setBit(a, i); }
		}	
		return a;
	}
	
	public static byte[] long2bytes(long a)
	{
		byte[] data = new byte[8];
		
		for(int i = 0; i < 64; i++)
		{
			boolean set = getBit(a, i);
			if(set)
			{
				int relbyte = (i / 8);
				data[relbyte] = setBit(data[relbyte], (i % 8));
			}
		}		
		return data;
	}
	
	public static int posHashCode(String s)
	{
		int hc = s.hashCode();
		hc = (hc < 0 ? -hc : hc);
		return hc;
	}
	
	public static void printStartFlagInfo(EtlJob ejob, String daycode)
	{
		printStartFlagInfo(ejob, daycode, System.out, System.err);	
	}
	
	public static void printEndFlagInfo(EtlJob ejob, String daycode)
	{
		printEndFlagInfo(ejob, daycode, System.out, System.err);	
	}	
	
	public static void printStartFlagInfo(EtlJob ejob, String daycode, PrintStream out, PrintStream err)
	{
		for(PrintStream ps : new PrintStream[] { out, err } )
		{
			ps.printf("%s\n", BIG_PRINT_BAR);
			ps.printf("%s__%s__START\n", daycode, ejob);			
		}
	}
	
	public static void printEndFlagInfo(EtlJob ejob, String daycode, PrintStream out, PrintStream err)
	{
		for(PrintStream ps : new PrintStream[] { out, err } )
		{
			ps.printf("%s__%s__END\n", daycode, ejob);						
			ps.printf("%s\n", BIG_PRINT_BAR);
		}		
	}	
	
	public static <T> T serializeCloneE(T toclone)
	{
		try  { return serializeClone(toclone); }
		catch (Exception ex) { throw new RuntimeException(ex); }
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T serializeClone(T toclone) throws Exception
	{
		Util.massert(toclone instanceof Serializable, "Object must implement Serializable interface");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		FileUtils.serialize(((Serializable) toclone), baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		return (T)FileUtils.unserialize(bais);
		// return clone;
	}	
	
	public static List<Integer> range(int n)
	{
		List<Integer> rlist = Util.vector();
		
		for(int i = 0; i < n; i++)
			{ rlist.add(i); }
		
		return rlist;
	}
	
	public static List<Integer> range(Collection c)
	{ 
		return range(c.size());	
	}
	
	public static List<Integer> range(Object[] a)
	{
		return range(a.length);	
	}
	
	public static void sleepEat(long millis)
	{
		try { Thread.sleep(millis); }
		catch (InterruptedException inex)  {} 
	}
	
	public static class SshUtil
	{
		public String username;
		public String hostname;
		public String rsapath;
				
		public SshUtil()
		{
			username = getUserName();
			rsapath = Util.sprintf("/home/%s/.ssh/my_private_key", username);
		}
		
		public String getSysCall(String rootcall)
		{
			Util.massert((new File(rsapath)).exists(),
				"RSA file not found: %s",  rsapath);
			
			return Util.sprintf("ssh -i %s %s@%s %s",
				rsapath, username, hostname, rootcall);		
		}
	}
	
	public interface LineReader
	{
		public abstract String readLine() throws IOException;
		public abstract void close() throws IOException;
	}	
	

	public enum LogField
	{
		date_time, ip, country, 
		region, request_uri, referer, useragent, 
		uuid, winner_price, ad_exchange, auction_id, impression_id,
		bid, advertiser_id, campaign_id, line_item_id, line_item_type,
		creative_id, size, adaptv_creative_type, auction_type, advertiser_pricing_type,
		url, domain, url_keywords, referrer_url, 
		referrer_domain, referrer_url_keywords, visibility, tag_format, 
		within_iframe, publisher_pricing_type, google_adslot_id, google_verticals, 
		google_main_vertical, google_main_vertical_with_weight, google_verticals_slicer,
		google_anonymous_id, 
		admeld_website_id, admeld_publisher_id, admeld_tag_id, 
		adnexus_tag_id, adnexus_inventory_class,
		openx_website_id, openx_placement_id, openx_category_tier1, openx_category_tier2,
		rubicon_website_id, rubicon_site_channel_id, rubicon_site_name, 
		rubicon_domain_name, rubicon_page_url,
		improve_digital_website_id, 
		yahoo_buyer_line_item_id, yahoo_exchange_id, yahoo_pub_channel, yahoo_seller_id,
		yahoo_seller_line_item_id, yahoo_section_id, yahoo_segment_id, yahoo_site_id,
		adbrite_zone_id, adbrite_zone_url, adbrite_zone_quality,
		adaptv_is_top, adaptv_placement_name, adaptv_placement_id, adaptv_placement_topics,
		adaptv_placement_quality, adaptv_placement_metrics, adaptv_video_id,
		casale_website_channel_id, casale_website_id,
		cox_content_categories,
		contextweb_categories, contextweb_tag_id,
		adjug_publisher_id, adjug_site_id,
		dbh_publisher_id, dbh_site_id, dbh_placement_id, dbh_ad_tag_type, dbh_default_type,
		nexage_content_categories, nexage_publisher_id, nexage_site_id,
		user_ip, user_country, user_region, user_DMA, user_city, user_postal, user_language,
		language, user_agent, browser, os, time_zone, exchange_user_id,
		wtp_user_id, view_count, no_flash, age, gender, ethnicity, marital, kids,
		hhi, is_test, load_i5m, load_i9h,
		no_sync, mobile_carrier, mobile_loc, mobile_device_id, 
		mobile_device_platform_id, mobile_device_make, mobile_device_model, mobile_device_js,
		conversion_type, 
		adnexus_reserve_price, adnexus_estimated_clear_price, adnexus_estimated_average_price,
		adnexus_estimated_price_verified, 
		adbrite_current_time, adbrite_previous_page_view_time,
		admeta_site_categories, admeta_site_id, spotx_categories, 
		real_domain, real_referrer_domain, real_url, real_referrer_url,
		reporting_type, currency, transaction_id, retargeting_timelimit,
		adscale_slot_id,
		liveintent_publisher_id, liveintent_site_id, liveintent_publisher_categories, liveintent_site_categories,
		ua_device_type, ua_device_maker, ua_device_model, ua_os, ua_os_version,
		ua_browser, ua_browser_version, is_mobile_app, utw,
		content, deal_id, deal_price, appnexus_creative_id, facebook_page_type,
		pubmatic_pub_id, pubmatic_site_id, pubmatic_ad_id, segment_info,
		compression_ratio,
		redirect_url, 
		conversion_id,
		is_post_view, is_post_click, dbh_section_id,
		// These are specific to pixel logs
		pixel_type, pixel_format, pixel_id, secure,
		
		/* New in v22 */
		user_agent_hash, 
		targeting_criteria, 
		iab_category,
		cookie_born,
		real_creative_id,
		impression_timestamp,
		cpa_amount,
		top_10_segments_flag,
		bid_floor_cpm,
		random_prefiltered_user,
		publisher_payout,
		conversion_interval,
		loss_reason_code,
		
		// These are DERIVED fields 
		hour(true), country_region(true), user_hour(true),
		
		
		dbh_macro;
		
		private boolean _isDerived;
		
		LogField(boolean isderv)
		{
			_isDerived = isderv;	
		}
		
		LogField()
		{
			this(false);	
		}
		
		public boolean isDerived()
		{
			return _isDerived;
		}
	}
	
	private static Set<LogField> DERIVED_FIELDS = Collections.synchronizedSet(new TreeSet<LogField>());
		
	static {
		DERIVED_FIELDS.add(LogField.hour);
		DERIVED_FIELDS.add(LogField.country_region);
		DERIVED_FIELDS.add(LogField.user_hour);
	}	
}

