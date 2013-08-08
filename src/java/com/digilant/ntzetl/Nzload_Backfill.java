
package com.digilant.ntzetl;

import java.io.*;
import java.util.*;
// import java.math.BigInteger;
import java.math.*;
import java.lang.Integer;
import java.text.SimpleDateFormat;
import java.security.*;
import java.util.regex.*;

import com.adnetik.shared.*;
import com.adnetik.shared.DbUtil.NZConnSource;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.BleStructure.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;
import com.digilant.mobile.MobileUtil;
import com.digilant.dbh.IABMap.*;
import com.digilant.ntzetl.SendMail.*;
import com.digilant.ntzetl.loadData.*;

public class Nzload_Backfill
{
    // why doesn't System.getenv(<VAR>) work for these?
    private static String NZ_HOST = "66.117.49.50";
    private static String NZ_DATABASE = "fastetl";
    private static String NZ_USER = "jaffer";
    private static String NZ_PASSWORD = "jaffer_101?";

    private ControlTable_hy _cTable;

    public static String BASE_PATH = System.getenv("HOME")+"/";

    private static String tkey;

    private static String tableName;

    private static String tsv_path;

    private static String adexchange;


    private static String _dayCode;

    private static Writer _logWriter;

    private SendMail _logMail;

    private static int timeerror = 0;

    private static void usage()
    {
	System.out.println("\nUsage: JavaCall.py Nzload_Backfill TABLE nbicca EXCHANGE [DATE] [COUNT]\n");
	System.exit(1);
    }

    public static void main(String[] args) throws Exception
    {
	String daycode;
	int cnt;
	if (args.length < 3 || args.length > 5) usage();
	tableName = args[0];
	tkey = args[1];
	adexchange = args[2];

	if (args.length == 3) daycode = TimeUtil.getYesterdayCode();
	else daycode = args[3];
	cnt = (args.length < 5) ? 1 : Integer.parseInt(args[4]);

	for (int i = 0; i < cnt; i++) {
	    Nzload_Backfill tzu = new Nzload_Backfill(daycode);
	    tzu.loadMany();
	    // if (failed > 0) tzu._logMail.send2admin();
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    Calendar c = Calendar.getInstance();
	    c.setTime(sdf.parse(daycode));
	    c.add(Calendar.DATE, 1); // number of days to add
	    // daycode = sdf.format(c.getTime()); // dt is now the new date
	    // System.out.println("Run next starting " + daycode);
	}
	//System.out.println(rownum + " records inserted; " + failed + " failed");
	System.exit(0);
    }

    public Nzload_Backfill(String dc)
    {
	TimeUtil.assertValidDayCode(dc);
	_dayCode = dc;
	_cTable = new ControlTable_hy.CleanListImpl(BASE_PATH+tableName+"/cleandir_"+adexchange);
    }

    private List<String> getNfsPathList(LogType logtypes)
    {
	List<String> pathlist = Util.vector();
	ExcName oneexc = null;
	if (adexchange.equals("dbh"))  oneexc = ExcName.dbh;
	else if (adexchange.equals("nexage")) oneexc = ExcName.nexage;
	else if (adexchange.equals("improvedigital")) oneexc = ExcName.improvedigital;
	else if (adexchange.equals("admeta")) oneexc = ExcName.admeta;
	else if (adexchange.equals("adbrite")) oneexc = ExcName.adbrite;
	else if (adexchange.equals("rtb")) oneexc = ExcName.rtb;
	else if (adexchange.equals("casale")) oneexc = ExcName.casale;
	else if (adexchange.equals("openx")) oneexc = ExcName.openx;
	else if (adexchange.equals("admeld")) oneexc = ExcName.admeld;
	else if (adexchange.equals("adnexus")) oneexc = ExcName.adnexus;
	else if (adexchange.equals("facebook")) oneexc = ExcName.facebook;
	else if (adexchange.equals("contextweb")) oneexc = ExcName.contextweb;
	else if (adexchange.equals("liveintent")) oneexc = ExcName.liveintent;
	else if (adexchange.equals("yahoo")) oneexc = ExcName.yahoo;
	else if (adexchange.equals("rubicon")) oneexc = ExcName.rubicon;
	else if (adexchange.equals("pubmatic")) oneexc = ExcName.pubmatic;
	else {
	    _logMail.pf(" unknown exchange: %s\n Exiting.\n", adexchange);
	    System.exit(17);
	}
	{
	    List<String> implist = Util.getNfsLogPaths(oneexc, logtypes, _dayCode);
	    if (implist != null)
		{ pathlist.addAll(implist); }
	}
	return pathlist;
    }

    private List<String> getNfsPathListdbh (LogType logtypes)
    {
	List<String> pathlist = Util.vector();
	List<String> implist = Util.getNfsLogPaths(ExcName.dbh, logtypes, _dayCode);
	if (implist != null)
	    { pathlist.addAll(implist); }
	return pathlist;
    }

    private void loadMany() throws Exception
    {
	int fcount = 0;
	int numcount=0;
	for (LogType ltp : new LogType[] {
		LogType.click,
		LogType.conversion,
		LogType.activity,
		LogType.imp,
		LogType.bid_all,
		LogType.no_bid
	    }) {
	    if (ltp==LogType.no_bid && tkey.charAt(0) != 'N' && tkey.charAt(0) != 'n') continue;
	    if (ltp==LogType.bid_all && tkey.charAt(1) != 'B' && tkey.charAt(1) != 'b') continue;
	    if (ltp==LogType.imp && tkey.charAt(2) != 'I' && tkey.charAt(2) != 'i') continue;
	    if (ltp==LogType.click && tkey.charAt(3) != 'C' && tkey.charAt(3) != 'c') continue;
	    if (ltp==LogType.conversion && tkey.charAt(4) != 'C' && tkey.charAt(4) != 'c') continue;
	    if (ltp==LogType.activity && tkey.charAt(5) != 'A' && tkey.charAt(5) != 'a') continue;
	    System.out.println("********" + ltp);
	    List<String> pathlist = getNfsPathList(ltp);
	    if (ltp==LogType.no_bid) {
		if (adexchange.equals("dbh"))
		    pathlist = getNfsPathListdbh(ltp);
		else continue;
	    }
	    else
		pathlist = getNfsPathList(ltp);

	    loadData data=new loadData(adexchange);

	    for (String onepath : pathlist) {

		tsv_path = System.getenv("HOME")+"/"+tableName+"/"+_dayCode+"-"+adexchange+"-"+ltp+".tsv";

		data.createSubFile(onepath,tsv_path);

		numcount+=data.CountBatch();

		if (numcount > 100000) { // 0
		    data.transferNLoad(tsv_path,tableName);
		    numcount=0;
		}
		fcount++;
	    }
	    if (numcount > 0)
		{ data.transferNLoad(tsv_path,tableName);}

	    _cTable.reportFinished(pathlist);
	    data.sendlogmail();
	}
    }
}
