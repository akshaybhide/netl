package com.digilant.fastetl;

import java.util.*;
import java.io.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.Util;
import com.digilant.fastetl.FastUtil.MyLogType;

import java.util.*;

public class FileManager
{
	// TODO: think about NFS write semantics, will the file be completely written??
	private static final Object lock = new Object();
	private Set<String> _cleanSet = Collections.synchronizedSet(new HashSet<String>());;
	public static ConfigWrapper config;
	public FileManager(String configpath)
	{
		config = new ConfigWrapper(configpath);
		
	}
	
	
	public void flushCleanList() throws IOException
	{
		// Write to the STAGING (false) path
		String cleanpath = getCleanListPath(false);
		FileUtils.createDirForPath(cleanpath);

		writeFileLines(_cleanSet, cleanpath);
		Util.pf("Wrote %d clean list files\n", _cleanSet.size());
	}
	
	public void flushCleanList(String filepath) throws IOException
	{
		// Write to the STAGING (false) path
		String cleanpath = getCleanListPath(false);
		FileUtils.createDirForPath(cleanpath);
		ArrayList<String> ar = new ArrayList<String>();
		ar.add(filepath);
		writeFileLines(ar, cleanpath);
		Util.pf("Wrote 1 file to clean list \n");
	}
	public static void writeFileLines(Collection<String> data, String path) throws IOException
	{
		FileWriter fw = new FileWriter(path, true);
		
		for(String s : data)
		{
			fw.write(s+"\n");	
		}
		
		fw.close();		
	}

	// Grab all the files in the last N days, remove those that have already been processed
	public Set<String> newFilesLookBack(String date, int lookback, MyLogType[] mlt)
	{
		Set<String> newset = getPathsSince(date, lookback, mlt);
		newset.removeAll(_cleanSet);
		return newset;
	}
	public Set<String> newFilesLookBack(String date, int lookback, MyLogType[] mlt, ExcName[] en )
	{
		Set<String> newset = getPathsSince(date, lookback, mlt, en);
		newset.removeAll(_cleanSet);
		return newset;
	}
	public void MoveToCurrent() throws Exception
	{
		{
			String jnkpath = getJunkPath();
			FileUtils.createDirForPath(jnkpath);
			FileUtils.createDirForPath(jnkpath);
			File jnk = new File(jnkpath);
			File cur = new File(getBaseDir(true));
			File stg = new File(getBaseDir(false));
			
			// TODO: delete junk directories
			
			// Should be very fast operation
			boolean cur_renamed = cur.renameTo(jnk);
			if(!cur_renamed)
				Util.pf("problem rename to junk path : %s \n", jnk.getAbsoluteFile());
			boolean stg_renamed = stg.renameTo(cur);
			if(!stg_renamed)
				Util.pf("problem rename to current path\n");
			if(stg_renamed && cur_renamed)
				Util.pf("renamed directory %s --> %s\n", stg, cur);			
		}
	}	


	
	public void reportFinished(String filepath)
	{
		Util.massert(!_cleanSet.contains(filepath), "error, file has already been processed %s", filepath);
		_cleanSet.add(filepath);	
	}
	
	public synchronized void reloadFromSaveDir()
	{
		//Util.massert(_bigAgg.get(MyLogType.imp).isEmpty());
		
		// Load clean list
		synchronized(lock)
		{
			String cleanpath = getCleanListPath(true);
			try{
				FileUtils.createDirForPath(cleanpath);
				List<String> cleanlines = FileUtils.readFileLines(cleanpath);
				_cleanSet.addAll(cleanlines);
				Util.pf("Read %d clean list file\n", _cleanSet.size());
				}catch(IOException e){
					Util.pf("No clean list found at %s\n", cleanpath);
					
				}
			
		}
		for(String iccpix : new String[] { "icc" , "pixel" })
		{
			File  basedir = new File(Util.sprintf("%s/%s", getBaseDir(true), iccpix));
			// 	AggregationEngine.cookie_agg.loadFromBaseDir(basedir);
		}
		
		// load general aggregators from file
		//cheesy way to get icc and pixel directories
		Set<String> aggpathset = getAggPathSet(true);
		
		MyLogType[] lt = new MyLogType[] { MyLogType.pixel, MyLogType.imp };
		for(MyLogType mtype: lt)
		{
			// List<String> filepaths = FastUtil.getQuatPaths(mtype);
			// AggregationEngine.loadFromFiles(filepaths);
			
			//File  basedir = new File(Util.sprintf("%s/%s", FastUtil.getQuatPath(mtype, relid));
		}
	}
	public synchronized void removeDateFromCleanList(String date){
		if(_cleanSet==null)
			return;
		if(_cleanSet.isEmpty())
			return;
		Set<String> delete = new HashSet<String>();
		synchronized(lock){
			for(String path:_cleanSet){
				if(path.contains(date))
					delete.add(path);
			}
			for(String d: delete){
				_cleanSet.remove(d);
			}
		}
	}
	
	public String getCookiePath(boolean is_current, MyLogType mlt, Integer relid)
	{
		return getGenericPath(is_current, mlt, relid, true);	
	}
	
	public String getQuatlyPath(boolean is_current, MyLogType mlt, Integer relid)
	{
		return getGenericPath(is_current, mlt, relid, false);
	}	
	
	private String getGenericPath(boolean is_current, MyLogType mlt, Integer relid, boolean is_cookie)
	{
/*		return Util.sprintf("%s/%s/%s/%d/%s.%s",
			BASE_PATH, 
			(is_current ? "current" : "staging"), 
			(mlt == MyLogType.pixel ? "pixel" : "icc"),
			relid, mlt, (is_cookie ? "cookie" : "quatly"));*/		
		return Util.sprintf("%s/%s/%s/%d/%s.%s",
				config.getDest(), 
				(is_current ? "current" : "staging"), 
				(mlt == MyLogType.pixel ? "pixel" : "icc"),
				relid, mlt, (is_cookie ? "cookie" : "quatly"));		
	}
	
	public String getBaseDir(boolean is_current)
	{
//		return Util.sprintf("%s/%s", BASE_PATH, (is_current ? "current" : "staging"));
		return Util.sprintf("%s/%s", config.getDest(), (is_current ? "current" : "staging"));
	}
	
	public String getCleanListPath(boolean is_current)
	{
//		return Util.sprintf("%s/%s/cleanlist.txt", BASE_PATH, (is_current ? "current" : "staging"));
		return Util.sprintf("%s/%s/cleanlist.txt", config.getDest(), (is_current ? "current" : "staging"));
	}
	public  String getJunkPath()
	{
		for(int i = 0; i < 100; i++)
		{
			Random r = new Random();	
			long rid = r.nextLong();
			rid = (rid < 0 ? -rid : rid);
			//String junkdir = Util.sprintf("%s/junk/junk_%d", BASE_PATH, rid);
			String junkdir = Util.sprintf("%s/junk/junk_%d", config.getDest(), rid);
			if(!(new File(junkdir)).exists())
				{ return junkdir;}
		}
		
		throw new RuntimeException("Could not find a good junk directory");
	}	

	public  String getStagingDir(MyLogType mlt, int relid)
	{
//		return Util.sprintf("%s/staging/%s/%d",
	//		BASE_PATH, (mlt == MyLogType.pixel ? "pixel" : "icc"), relid);
		return Util.sprintf("%s/staging/%s/%d",
				config.getDest(), (mlt == MyLogType.pixel ? "pixel" : "icc"), relid);
	}
	
	public static Calendar getCalendar4Date(String date){
		String[] parts = date.split("-");
		GregorianCalendar cal = new GregorianCalendar();
		cal.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])-1, Integer.parseInt(parts[2]), 1, 1, 1);
		return cal;
	}
	public static String getString4Calendar(Calendar cal){
		GregorianCalendar calendar = (GregorianCalendar)cal;
		String strmonth = ((calendar.get(Calendar.MONTH)+ 1) < 10)?"0"+(calendar.get(Calendar.MONTH)+ 1):""+(calendar.get(Calendar.MONTH)+ 1);
		String strday = (calendar.get(Calendar.DATE) < 10)?"0"+calendar.get(Calendar.DATE):""+calendar.get(Calendar.DATE);
		String strdate = ""+ calendar.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;	
		return strdate;
	}
	static Set<String> getPathsSince(String date, int numdays, MyLogType[] mlt)
	{
		GregorianCalendar cal = (GregorianCalendar)getCalendar4Date(date);
		GregorianCalendar past = (GregorianCalendar) cal.clone();
		cal.add(Calendar.DATE, 1);
		past.add(Calendar.DATE, -numdays +1);
		ArrayList<String> dayrange = new ArrayList<String>();
		while(past.before(cal)){
			dayrange.add(getString4Calendar(past));
			past.add(Calendar.DATE, 1);
		}
		Set<String> pathset = Util.treeset();
		
		for(String oneday : dayrange)
			{ pathset.addAll(getPathsForDay(oneday, mlt));}


		Util.pf("Read %d paths for daylist %s\n", pathset.size(), dayrange.toString());
		return pathset;
	}
	static Set<String> getPathsSince(String date, int numdays, MyLogType[] mlt, ExcName[] en)
	{
		GregorianCalendar cal = (GregorianCalendar)getCalendar4Date(date);
		GregorianCalendar past = (GregorianCalendar) cal.clone();
		cal.add(Calendar.DATE, 1);
		past.add(Calendar.DATE, -numdays +1);
		ArrayList<String> dayrange = new ArrayList<String>();
		while(past.before(cal)){
			dayrange.add(getString4Calendar(past));
			past.add(Calendar.DATE, 1);
		}
		Set<String> pathset = Util.treeset();
		
		for(String oneday : dayrange)
			{ pathset.addAll(getPathsForDay(oneday, mlt, en));}


		Util.pf("Read %d paths for daylist %s\n", pathset.size(), dayrange.toString());
		return pathset;
	}
	
	static public Set<String> getPathsForDay(String daycode, MyLogType[] mlt)
	{
		Set<String> pathset = Util.treeset();

		for(MyLogType ltype : mlt)
		{
			if(ltype == MyLogType.pixel)
			{
				addIfNonNull(pathset, getNfsPixelPaths(daycode));
				
			} else {

				for(ExcName oneexc : ExcName.values())
					{ addIfNonNull(pathset, /*Util.*/getNfsLogPaths(oneexc, LogType.valueOf(ltype.toString()), daycode)); }
			}
		}
		
		return pathset;
	}	
	static public Set<String> getPathsForDay(String daycode, MyLogType[] mlt, ExcName[] en)
	{
		Set<String> pathset = Util.treeset();

		for(MyLogType ltype : mlt)
		{
			if(ltype == MyLogType.pixel)
			{
				addIfNonNull(pathset, getNfsPixelPaths(daycode));
				
			} else {

				for(ExcName oneexc : en)
					{ addIfNonNull(pathset, /*Util.*/getNfsLogPaths(oneexc, LogType.valueOf(ltype.toString()), daycode)); }
			}
		}
		
		return pathset;
	}	
	private static void addIfNonNull(Collection<String> addto, Collection<String> addfrom)
	{
		if(addfrom != null)
			{ addto.addAll(addfrom);}
	}

	public static List<String> getNfsLogPaths(ExcName excname, LogType logtype, String daycode)
	{
		List<String> lpaths = Util.vector();
		// /mnt/adnetik/adnetik-uservervillage/adnexus/userver_log/no_bid/2011-10-24/2011-10-24-23-59-59.EDT.no_bid_v12.adnexus-rtb-ireland_5e37d.log.gz

		String dirPath = getNfsDirPath(excname, logtype, daycode);
		
		//Util.pf("Dir path is %s", dirPath);
		
		File dirFile = new File(dirPath);
		if(!dirFile.exists()|| !dirFile.isDirectory())
			{ 	Util.pf("Dir path %s did not exist or was not a directory\n", dirPath);
				return null; }
		
		String[] subfiles = dirFile.list();
		if(subfiles == null)
			return lpaths;
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
//		return Util.sprintf("%s/%s/userver_log/%s/%s/", USERVER_NFS_PATH, excname, logtype, daycode);
		String logtypestr = logtype.toString();
		return Util.sprintf("%s/%s/userver_log/%s/%s/", config.getICCSrc(), excname, logtypestr, daycode);
	}

	static String getInterestPath(){
		//return BASE_PATH ;
		return config.getDest();
	}

	
	public Set<String> getAggPathSet(boolean is_current)
	{
		File basedir = new File(getBaseDir(is_current));
		Set<String> pathset = Util.treeset();
		recAddPaths(pathset, basedir, null);
		return pathset;
	}
	public Set<String> getAggPathByRelID(boolean is_current, MyLogType ltype ,Integer relID)
	{
		File basedir = new File(getBaseDir(is_current));
		Set<String> pathset = Util.treeset();
		PathIncludes filter = new PathIncludes(ltype == MyLogType.pixel?"pixel":"icc",new String[]{""+relID});
		recAddPaths(pathset, basedir, filter);
		return pathset;
	}
	
	
	static void recAddPaths(Set<String> pathset, File mydir, FilenameFilter filter)
	{
		File[] files;
		if(filter!=null)
			files = mydir.listFiles(filter);
		else
			files = mydir.listFiles();
		if(files == null) return;
		for(File subfile : files)
		{
			if(subfile.isDirectory())
				{ recAddPaths(pathset, subfile, filter);}
			
			pathset.add(subfile.getAbsolutePath());
		}
	}
	public static List<String> getNfsPixelPaths(String daycode)
	{
		List<String> lpaths = Util.vector();
		
		// /mnt/adnetik/adnetik-uservervillage/adnexus/userver_log/no_bid/2011-10-24/2011-10-24-23-59-59.EDT.no_bid_v12.adnexus-rtb-ireland_5e37d.log.gz
		
		//String dirPath = getNfsDirPath(excname, logtype, daycode);
		///mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/
		//USERVER_NFS_PATH = "/mnt/adnetik/adnetik-uservervillage";
		
		String dirPath = Util.sprintf("%s/%s/", config.getPixelSrc(), daycode);
		//String dirPath = Util.sprintf("%s/rtb/userver_log/pixel/%s/", FastUtil.USERVER_NFS_PATH, daycode);
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
		


}
