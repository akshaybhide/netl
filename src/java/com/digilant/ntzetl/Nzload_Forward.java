
package com.digilant.ntzetl;

import java.io.*;
import java.util.*;
import java.math.BigInteger;
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

public class Nzload_Forward
{
    // why doesn't System.getenv(<VAR>) work for these?
    private static String NZ_HOST = "66.117.49.50";
    private static String NZ_DATABASE = "fastetl";
    private static String NZ_USER = "jaffer";
    private static String NZ_PASSWORD = "jaffer_101?";

    private static String tableName;
    private String  _adexchange;
    private ControlTable_hy _cTable;

    public static String BASE_PATH = System.getenv("HOME")+"/";


    public static void main(String[] args) throws Exception
    {
	tableName = args[0];
	String exchange = args[1];
	//String daycode = args[2];
	//daycode = "yest".equals(daycode) ? daycode : TimeUtil.getYesterdayCode() ;
	Nzload_Forward tzu = new Nzload_Forward(tableName,exchange);
	tzu.autorun();
    }

    public Nzload_Forward(String table,String adexc)
    {
	//TimeUtil.assertValidDayCode(dc);
	//********for cleanlist
	_cTable = new ControlTable_hy.CleanListImpl(BASE_PATH+table+"/cleandir_"+adexc);
	//************************
	_adexchange = adexc;
    }

    void autorun() throws Exception
    {
	Util.pf("Look back is %d days\n", SliUtil_hy.LOOKBACK);
	ExcName oneexc=ExcName.valueOf(_adexchange);
	System.out.println(oneexc.toString());
	int fallbehind=1;

	while (true) {
	    while (fallbehind==0) {
		Thread.sleep(5*60*1000); //5 minutes
		fallbehind=1;
	    }
	    Util.pf("Next block to run for"+_adexchange);
	    SortedSet<String> pathset = _cTable.nextBatch(2, 500,oneexc);
	    fallbehind=_cTable.getFallBehind();
	    if (pathset.size()>0) {
		loadMany(pathset,_adexchange);
		//if (failed>0) _logMail.send2admin();
		_cTable.reportFinished(pathset);
	    }
	    else
		Util.pf("no path for this block\n");
	}
    }

    private void loadMany(Collection<String> pathlist, String exc) throws Exception
    {
	int fcount = 0;

	for (String onepath : pathlist) {
	    PathInfo pinfo = new PathInfo(onepath);
	    ExcName exchange=pinfo.pExc;
	    LogType ltp=pinfo.pType;
	    String day = pinfo.getDate();

	    System.out.println("adexchange is  :"+_adexchange);

	    String tsv_path = System.getenv("HOME")+"/"+tableName+"/"+day+"-"+exchange.toString()+"-"+ltp+".tsv";

	    System.out.println(tsv_path);

	    if (ltp==LogType.no_bid) {
		if (exchange==ExcName.dbh) {}
		else continue;
	    }
	    //createSubFile(onepath);
	    //transferNLoad();
	    loadData data = new loadData(exc);
	    boolean test = data.createSubFile(onepath,tsv_path);
	    int w = 0;
	    while (test==false && w<4) {
		System.out.println("probably permission denied, gonna sleep for 5 minutes");
		Thread.sleep(5*60*1000);
		test=data.createSubFile(onepath,tsv_path);
		w++;
	    }
	    data.transferNLoad(tsv_path,tableName);
	    data.sendlogmail();
	}
    }
}
