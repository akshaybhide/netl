package com.digilant.mobile;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Random;

import javax.sql.rowset.CachedRowSet;

import com.digilant.fastetl.FileManager;
import com.adnetik.shared.Util;
public class HAggregator {
	private static  LinkedHashMap<String, String>  colnames_and_types;
	private static ArrayList<String> colnames;
	private static String _machine;
	private static String _db;
	private static String _srctable;
	private static String _desttable;
	private static String _constpart;
	public static void init(String machine,String db, String srctable, String desttable) throws SQLException{
		colnames_and_types = DBConnection.lookupColumnsAndTypes(machine, db, desttable);
		colnames = DBConnection.lookupColumns(machine, db, desttable);
		_machine = machine;
		_db = db;
		_srctable = srctable;
		_desttable = desttable;
		_constpart = colnames.toString().replace('[', '(').replace("]", ")").replace("ID_TIME,","");
	}
	public static void cleanup(String date, int lookback){
		GregorianCalendar cal = (GregorianCalendar) FileManager.getCalendar4Date(date);
		for(int i = 0; i < lookback; i++){
			cal.add(Calendar.DATE, -1);
			String strdate = FileManager.getString4Calendar(cal);
			try {
					report(strdate, 23, 3, false);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
		}
	}
	
	public static void report(String date, int hour, int latest_quarter, boolean leavelast15) throws SQLException, IOException{
		int lq = latest_quarter;
		Connection mconn = DBConnection.getConnection(_machine, _db);
		for(int h = 0;h<=hour;h++ ){
			System.out.println("reporting for date " + date+ " and hour "+ h);
			latest_quarter = 3;
			if(h == hour) latest_quarter= lq;
			String query =  
			"select "+
			"_DATE," +
			"ID_ADVERTISER,"+
			"ID_EXCHANGE,"+
			"ID_METROCODE,"+
			"ID_REGION,"+
			"ID_COUNTRY,"+
			"ID_LINEITEM,"+
			"ID_CAMPAIGN,"+
			"ID_CREATIVE,"+
			"ID_SIZE,"+
			"ID_VISIBILITY,"+
			"ID_BROWSER,"+
			"ID_LANGUAGE,"+
			"ID_CURRCODE,"+
			"ID_DEAL,"+
			"ID_FB_PAGETYPE,"+
			"DEVICE_TYPE,"+
			"DEVICE_MODEL,"+
			"DEVICE_MAKER,"+
			"OS,"+
			"OS_VERSION,"+
			"UA_BROWSER,"+
			"UA_BROWSER_VERSION,"+
			"IS_MOBILE_APP,"+
			"sum(NUM_BIDS),"+
			"sum(NUM_CLICKS),"+
			"sum(NUM_IMPRESSIONS),"+
			"sum(NUM_CONVERSIONS),"+
			"sum(NUM_CONV_POST_VIEW),"+
			"sum(NUM_CONV_POST_CLICK),"+
			"sum(IMP_COST),"+
			"sum(IMP_BID_AMOUNT),"+
			"sum(IMP_DEAL_PRICE),"+
			"ENTRY_DATE,"+
			"EXT_LINEITEM,"+
			"ID_ASSIGNMENT,"+
			"usertext1,"+
			"usertext2,"+
			"usertext3,"+
			"usertext4,"+
			"userint1,"+
			"userint2,"+
			"userint3,"+
			"userint4,"+
			"userdecimal1,"+
			"userdate1"+
			" from "+ _srctable +" where _DATE="+ MobileUtil.encloseInSingleQuotes(date)+ " and ID_TIME <= " + GeneralDimension._hmCatalog.get("ID_TIME").get(h+""+ latest_quarter) + " and ID_TIME >= "+GeneralDimension._hmCatalog.get("ID_TIME").get(h+"0")+
			" group by ID_ADVERTISER,ID_EXCHANGE,ID_METROCODE,ID_REGION,ID_COUNTRY,ID_LINEITEM,ID_CAMPAIGN,ID_CREATIVE,ID_SIZE,ID_VISIBILITY,ID_BROWSER,ID_LANGUAGE,ID_CURRCODE, ID_DEAL, ID_FB_PAGETYPE, DEVICE_TYPE,DEVICE_MODEL,DEVICE_MAKER,OS,OS_VERSION,UA_BROWSER,UA_BROWSER_VERSION,IS_MOBILE_APP,ENTRY_DATE,EXT_LINEITEM,ID_ASSIGNMENT,usertext1,usertext2,usertext3,usertext4,userint1,userint2,userint3,userint4,userdecimal1,userdate1";
			//System.out.println(query);
			CachedRowSet rs = DBConnection.runBatchQuery(mconn, query);
			String val;	
			int colno = 1;
			ArrayList<String> ins_values = new ArrayList<String>();
			while(rs.next()){
				StringBuffer ins_row = new StringBuffer();
				ins_row.append("(");
				boolean is_cc = false;
				for(int i = 1; i <= colnames.size()-3; i++){
					String tmp = rs.getString(i);
					if(tmp == null)
							tmp = "NULL";
					if(i==24||i==26)
						is_cc=is_cc||!tmp.trim().equals("0");
					if(i>1)
						ins_row.append(",");
					if((colnames_and_types.get(colnames.get(i-1)).toLowerCase().contains("varchar")||colnames.get(i-1).toLowerCase().contains("date"))&& !tmp.equals("NULL"))
						ins_row.append(MobileUtil.encloseInSingleQuotes(tmp));
					else
						ins_row.append(tmp);
				}
				ins_row.append(",");
				ins_row.append(is_cc);
				ins_row.append(",");
				ins_row.append(rand99());
				ins_row.append(",");
				ins_row.append(h);
				ins_row.append(")");
				ins_values.add(ins_row.toString());
				
			}
		if(ins_values.size() == 0) {
			continue;
			}
		StringBuffer q = new StringBuffer();
		q.append("insert into ");
		q.append(_desttable);
		q.append(_constpart);
		q.append(" values ");
		int total = 0 ;
		int i = 1;
		ArrayList<Integer> cnt = new ArrayList<Integer>();
		cnt.add(0);

		for(String v : ins_values){
			if(i > 1)
				q.append(",");
			q.append(v);
			i++;
			if(i%1000 == 0 ){
				//System.out.println(q.toString());
				DBConnection.runBatchQuery(mconn, q.toString(), cnt);
				if(i-1 != cnt.get(0))
					Util.pf("!!!Expected to insert %d records but insrted %d \n", i - 1, cnt.get(0));
				total+=i-1;
				i = 1;cnt.clear();
				cnt.add(0);
				q = new StringBuffer();
				q.append("insert into ");
				q.append(_desttable);
				q.append(_constpart);
				q.append(" values ");
			}

		}
		if(i > 1){
			cnt.clear();
			cnt.add(0);
			//System.out.println(q.toString());
			DBConnection.runBatchQuery(mconn, q.toString(), cnt);
			if(ins_values.size()-total!=cnt.get(0))
				Util.pf("!!!Expected to insert %d records but insrted %d \n", ins_values.size() - total, cnt.get(0));
		}
/*		ins_values.clear();
		ins_values.add(q.toString());
		FileUtils.createDirForPath("/home/armita/q/test.txt");
		FileUtils.writeFileLines(ins_values, "/home/armita/q/test.txt");*/
		
		q = new StringBuffer();
		q.append("delete  from ");
		q.append(_srctable);
		q.append(" where  ");
		q.append("_DATE=");
		q.append(MobileUtil.encloseInSingleQuotes(date));
		q.append(" and ID_TIME <= ");
		//leaving the latest 15 minutes data
		if(leavelast15&&h==hour)
			latest_quarter--;
		q.append(GeneralDimension._hmCatalog.get("ID_TIME").get(h+"" + latest_quarter));
		q.append(" and ID_TIME >= ");
		q.append(GeneralDimension._hmCatalog.get("ID_TIME").get(h+"0"));
		//System.out.println(q.toString());
		DBConnection.runBatchQuery(mconn, q.toString());
		}
		mconn.close();
	}
	public static int rand99(){
		Random r = new Random();
		return r.nextInt(100);
	}
	public static void RemoveData4Date(String date){
		Connection mconn;
		System.out.println("removing data for date : " + date);
		try {
			mconn = DBConnection.getConnection(_machine, _db);
			StringBuffer q = new StringBuffer();
			q.append("delete  from ");
			q.append(_desttable);
			q.append(" where  ");
			q.append("ENTRY_DATE=");
			q.append(MobileUtil.encloseInSingleQuotes(date));
			DBConnection.runBatchQuery(mconn, q.toString());
			
			q = new StringBuffer();
			q.append("delete  from ");
			q.append(_srctable);
			q.append(" where  ");
			q.append("ENTRY_DATE=");
			q.append(MobileUtil.encloseInSingleQuotes(date));
			DBConnection.runBatchQuery(mconn, q.toString());
			q = new StringBuffer();
			q.append("delete  from processed_dates ");
			q.append(" where  ");
			q.append("p_date=");
			q.append(MobileUtil.encloseInSingleQuotes(date));
			DBConnection.runBatchQuery(mconn, q.toString());
			mconn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void AddDate2DB(String date){
		Connection mconn;
		try {
			mconn = DBConnection.getConnection(_machine, _db);
			StringBuffer q = new StringBuffer();
			q.append("replace into  processed_dates values (");
			q.append(MobileUtil.encloseInSingleQuotes(date));
			q.append(")");
			DBConnection.runBatchQuery(mconn, q.toString());
			mconn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void WrapUp2DB(String date){
		Connection mconn;
		try {
			mconn = DBConnection.getConnection(_machine, _db);
			StringBuffer q = new StringBuffer();
			q.append("replace into  wrapped_up values (");
			q.append(MobileUtil.encloseInSingleQuotes(date));
			q.append(")");
			DBConnection.runBatchQuery(mconn, q.toString());
			mconn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static boolean isWrappedUp(String date){
		try {
			String query = "select * from wrapped_up where w_date='" + date+"'";
			CachedRowSet rs = DBConnection.runQuery(_machine, _db, query);
			return rs.next();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}



}
