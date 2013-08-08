package com.digilant.dbh;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;


import com.adnetik.shared.Util;

public class AggregationWrapper {
	public String machine;
	public String db;
	public static String dest_table;
	public String info_table;
	public static String mode = "";
	public AggregationWrapper(String machine, String db, String info_table, String dest_table) throws SQLException{
		this.machine = machine;
		this.db = db;
		this.dest_table = dest_table;
		this.info_table = info_table;
	}
	public static void main(String[] args) throws Exception
	{
		if(args.length < 5){
			Util.pf( "you need to pass date (as 2012-05-29) and the number of look back date  and lookforward and path for config and ip\n");
			System.exit(1);
		}
		String initdate = args[0];
		int lookback = Integer.parseInt(args[1]);
		int lookfw = Integer.parseInt(args[2]);
		String configpath = args[3];
		String ip = args[4];
//		AggregationWrapper aw = new AggregationWrapper("66.117.49.100", "dbh", "dbh_dimensions", "dbh_general2");
		AggregationWrapper aw = new AggregationWrapper(ip, "dbh", "dbh_dimensions", "dbh_general2");
		aw.schedule(initdate,lookback, lookfw, configpath);
	}
	public void schedule(String initdate, int lookback,  int lookfw, String configpath) throws Exception{
		DBHReporter.init(machine, db,dest_table,info_table);
		
		// BACK FILL
		if(lookfw==0){
			GregorianCalendar pastfuture = new GregorianCalendar();
			int year = Integer.parseInt(initdate.split("-")[0]);
			int month = Integer.parseInt(initdate.split("-")[1]);
			int day = Integer.parseInt(initdate.split("-")[2]);
			pastfuture.clear();
			pastfuture.set(year, month - 1, day);
			GregorianCalendar pastpast = (GregorianCalendar)pastfuture.clone();
			pastpast.add(Calendar.DATE, -1*lookback);
			while(pastfuture.after(pastpast)){
				String strmonth = ((pastfuture.get(Calendar.MONTH)+ 1) < 10)?"0"+(pastfuture.get(Calendar.MONTH)+ 1):""+(pastfuture.get(Calendar.MONTH)+ 1);
				String strday = (pastfuture.get(Calendar.DATE) < 10)?"0"+pastfuture.get(Calendar.DATE):""+pastfuture.get(Calendar.DATE);
				String strdate = ""+ pastfuture.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
				Util.pf("processing date : %s\n" ,strdate);
				DBHReporter pr = new DBHReporter(strdate, 1, configpath, machine, db, dest_table, info_table);
				//pr.wrapup();
				pastfuture.add(Calendar.DATE, -1);
			}
		}
		//AggregationEngine aggEng = new AggregationEngine(initdate, lookback, false, configpath);
		//aggEng.wrapup();
		
		if(lookfw > 0)
		{
			//commented out to work with cronjobs
			//GregorianCalendar future = new GregorianCalendar();
			//int year = Integer.parseInt(initdate.split("-")[0]);
			//int month = Integer.parseInt(initdate.split("-")[1]);
			//int day = Integer.parseInt(initdate.split("-")[2]);
			//GregorianCalendar past = (GregorianCalendar)future.clone();
			GregorianCalendar now = new GregorianCalendar();
			//future.add(Calendar.DATE, lookfw);
		//	past.set(year, month - 1, day);
			//past.add(Calendar.DATE, -1*lookback);
			//while(past.before(future)){
				GregorianCalendar calendar = new GregorianCalendar();
				String strmonth = ((now.get(Calendar.MONTH)+ 1) < 10)?"0"+(now.get(Calendar.MONTH)+ 1):""+(now.get(Calendar.MONTH)+ 1);
				String strday = (now.get(Calendar.DATE) < 10)?"0"+now.get(Calendar.DATE):""+now.get(Calendar.DATE);
				String strdate = ""+ now.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
				Util.pf("processing date : %s\n" ,strdate);
				//int lb = FastUtil.CompareDates(TimeUtil.dayCode2Cal(strdate), TimeUtil.dayCode2Cal(TimeUtil.getTodayCode()));
				DBHReporter pr = new DBHReporter(strdate, 2, configpath, machine, db, dest_table, info_table);
				//String today = TimeUtil.getTodayCode();
				//if(today.equals(strdate)){
					//Thread.sleep(900000);
				//}
				//past.add(Calendar.DATE, 1);
				//if(past.after(calendar))
					//past = (GregorianCalendar)calendar.clone();
			//}

		}

		DBHReporter.wrapup();
		

	}

}
