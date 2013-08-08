
package com.digilant.ntzetl;

import java.sql.*;
import java.util.*;
import java.io.*;

import java.math.BigInteger;

// import org.apache.hadoop.mapred.*;
/*
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
*/

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.bm_etl.BmUtil.*;
import com.adnetik.bm_etl.*;

public class NzUtil
{
    // TODO: ext_lineitem...?
    public enum PseudoDimCode { entry_date, has_cc, rand99 };

    public enum LookupType { fourbyte, twobyte, none };

    public static String BASE_PATH = System.getenv("HOME")+"/";
    public static final String CLEAN_LIST_DIR = BASE_PATH + "cleandir/";
    public static final String CLICK_SAVE_DIR = BASE_PATH + "clicksave/";

    // This is a path to put sorted data for the disk version of LocalBatch
    public static String TEMP_KV_PATH_GIMP = BASE_PATH + "KEY_VALUE_GIMP.txt";
    // public static String TEMP_INF_PATH = BASE_PATH + "INF_DATA.txt";

    private static boolean IS_PRODUCTION = true;

    //public static final int LOOKBACK = 7;
    public static final int LOOKBACK = 3;

    public static final int CLEANLIST_SAVE_WINDOW = LOOKBACK+3;

    static { Util.massert(LOOKBACK < CLEANLIST_SAVE_WINDOW) ; }

    public static final int MAX_BATCH = 500;

    private static Random _INF_RANDOM = new Random();

    public static String getKvTempPath(AggType atype)
    {
	return Util.sprintf("%sKEY_VALUE_%s.txt", BASE_PATH, atype);
    }

    public static String getKvCsvLogPath(AggType atype, String daycode)
    {
	return Util.sprintf("%skvcsvlog/%s___%s.csv", BASE_PATH, atype, daycode);

    }

    static interface UploaderInterface
    {
	public int getTotalIn();
	public int getTotalUp();

	public void finish() throws IOException;

	public void send(AggType atype, Map<String, String> pmap, Metrics onemet) throws IOException;
    }

    public static class UploaderList implements UploaderInterface
    {
	private List<UploaderInterface> _upList = Util.vector();

	public UploaderList(UploaderInterface... ulist)
	{
	    // Util.massert(ulist.length >= 2, "Must supply at least two, otherwise just use the thing itself");

	    for(UploaderInterface uint : ulist)
		{ _upList.add(uint); }
	}

	public int getTotalIn()
	{
	    return _upList.get(0).getTotalIn();
	}

	public int getTotalUp()
	{
	    return _upList.get(0).getTotalUp();
	}

	public void send(AggType atype, Map<String, String> pmap, Metrics onemet) throws IOException
	{
	    for(UploaderInterface uint : _upList)
		{  uint.send(atype, pmap, onemet);	}
	}


	public void finish() throws IOException
	{
	    for(UploaderInterface uint : _upList)
		{  uint.finish(); }
	}

    }

    // TODO: where is this running...?
    // TODO: put this in the NightlyRunner
    static void runSlowGlobalUpdate(String daycode)
    {
	{
	    String delsql = Util.sprintf("DELETE FROM slow_global WHERE TheDate = '%s'", daycode);
	    DbUtil.execWithTime(delsql, "SLOW GLOBAL DELETE PREV", new DatabaseBridge(DbTarget.internal));
	}

	{
	    String[] colnames = new String[] { "NUM_BIDS", "NUM_CLICKS", "NUM_IMPRESSIONS", "NUM_CONVERSIONS", "NUM_CONV_POST_VIEW", "NUM_CONV_POST_CLICK", "IMP_COST", "IMP_BID_AMOUNT" };
	    List<String> explist = Util.vector();

	    for(String onecol : colnames)
		{ explist.add(Util.sprintf("SUM(%s) AS %s", onecol, onecol)); }

	    String insql = Util.sprintf("INSERT INTO slow_global SELECT TheDate, ID_COUNTRY, %s", Util.join(explist, ","));
	    insql += Util.sprintf(" FROM fast_general fg, dim_Date dd WHERE PK_Date = ID_DATE AND TheDate = '%s' GROUP BY TheDate, ID_COUNTRY ", daycode);

	    // Util.pf("Insert SQL is \n%s\n", insql);
	    DbUtil.execWithTime(insql, "SLOW GLOBAL INSERT NEW", new DatabaseBridge(DbTarget.internal));
	}
    }


    public static String getCleanListPath(String daycode)
    {
	return Util.sprintf("%s%s%sclean.txt", CLEAN_LIST_DIR, daycode, Util.DUMB_SEP);
    }

    public static String pathSimpleName(String x)
    {
	int lastslash = x.lastIndexOf("/");
	return x.substring(lastslash+1);
    }


    /*
      public static boolean isProd()
      {
      return  IS_PRODUCTION;
      }
    */

    public static String getShardPath(String daycode, int campaignid)
    {
	return Util.sprintf("%sshard/%s/camp_%d.txt", BASE_PATH, daycode, campaignid);
    }

    // TODO: this code is replicated all over the place
    static Set<String> getPathsForDay(String daycode)
    {
	Set<String> pathset = Util.treeset();

	for(LogType ltype : new LogType[] { LogType.bid_all, LogType.imp, LogType.click, LogType.conversion })
	    {
		for(ExcName oneexc : ExcName.values())
		    {
			List<String> dirlist = Util.getNfsLogPaths(oneexc, ltype, daycode);
			if(dirlist != null)
			    { pathset.addAll(dirlist); }
		    }
	    }

	return pathset;
    }

    public static String getNewTempInfPath(AggType atype)
    {
	// String bpath = BASE_PATH
	for(int i = 0; i < 1000; i++)
	    {
		Long rlong = _INF_RANDOM.nextLong();
		rlong = (rlong < 0 ? -rlong : rlong);
		String bpath = Util.sprintf("%sslicerep/infdata/%s_%d.txt",
					    BASE_PATH, atype, rlong);

		if(!(new File(bpath)).exists())
		    { return bpath; }
	    }

	throw new RuntimeException("Failed to find good inf path");
    }

    public static String getLogStatsDir(String daycode)
    {
	return Util.sprintf("%slogstats/%s", BASE_PATH, daycode);
    }

    public static String getNewStatLogPath(String daycode, QuarterCode qcode)
    {
	for(int i = 0; i < 100; i++)
	    {
		String probe = Util.sprintf("%slogstats/%s/log_%s%s.txt", BASE_PATH, daycode, qcode.toTimeStamp(), (i==0 ? "" : "__"+i));
		probe = probe.replace(":", "_");

		if(!(new File(probe)).exists())
		    { return probe; }
	    }

	throw new RuntimeException("Valid stat log path not found after 100 tries");
    }

    public static List<Integer> getCampListForPack(int maxCampId, int packid)
    {
	int NUM_CAMP_PACK = 16;

	Util.massert(0 <= packid && packid < 16, "Package ID must be 0 <= x < 16");

	List<Integer> packlist = Util.vector();
	for(int campid = packid; campid < maxCampId; campid += NUM_CAMP_PACK)
	    {
		packlist.add(campid);
	    }
	return packlist;
    }
    public static boolean LoadCommand(String filepath, String NZ_USER,String NZ_PASSWORD, String NZ_HOST, String NZ_DATABASE,String tabname, SimpleMail _logMail) throws IOException
    {
	boolean errs = false;
	String nzload = Util.sprintf("nzload -host %s -u %s -pw %s -db %s -t %s -maxErrors 100 -df %s",
				     NZ_HOST,
				     NZ_USER,
				     NZ_PASSWORD,
				     NZ_DATABASE,
				     tabname,
				     filepath);
	// System.out.println(nzload);
	//_logMail.pf("Initiating disk-NZ load...\n");

	List<String> outlist = Util.vector();
	List<String> errlist = Util.vector();

	double startup = Util.curtime();
	boolean load=Util.syscall(nzload, outlist, errlist);
	//logmail.pf(" ... done, %d output lines, %d error lines, took %.03f\n",
	//	outlist.size(), errlist.size(), (Util.curtime()-startup)/1000);
	if(load==true)
	for (String oneerr : errlist) {
	    //logmail.pf("ERROR: %s\n", oneerr);
	    _logMail.pf("nzload says:%s\n", oneerr);
	    errs = true;
	}
		return errs;
    }


    static public List<BigInteger> wtptrans(String pixid)
    {

	//System.out.println(pixid);
	List<BigInteger> list=Util.vector();
	if(pixid.isEmpty() || pixid.length() < 36){
	    return list;
	}
	String number="";
	while(pixid.indexOf("-")!=-1)
	    {      int i=pixid.indexOf("-");
		//	System.out.println(i);System.out.println("**********");
		String test=pixid.substring(0,i);
		//	System.out.println(test);  //System.out.println(pixid.substring(i+3,pixid.length()));

		pixid=pixid.substring(i+1,pixid.length());
		number+=test;
	    }
	number+=pixid; //System.out.println("**"+number+"**");
	//System.out.println(number.substring(0,16));System.out.println(number.substring(16,32));
	BigInteger left = new BigInteger(number.substring(0,16),16); //System.out.println(left);
	BigInteger right = new BigInteger(number.substring(16,32),16); //System.out.println(right);
	left=left.subtract(BigInteger.ONE.shiftLeft(63)); right=right.subtract(BigInteger.ONE.shiftLeft(63));

	list.add(left);
	list.add(right);

	return list;
    }
}
