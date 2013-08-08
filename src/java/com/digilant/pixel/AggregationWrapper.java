package com.digilant.pixel;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.adnetik.shared.TimeUtil;
import com.adnetik.shared.Util;

public class AggregationWrapper {
	public String machine;
	public String db;
	public static String dest_table;
	public String info_table;
	public static String mode = "";
	public String db_type;
	public AggregationWrapper(String dbtype, String machine, String db, String info_table, String dest_table) throws SQLException{
		this.machine = machine;
		this.db = db;
		this.dest_table = dest_table;
		this.info_table = info_table;
		this.db_type = dbtype;
	}
	public static void main(String[] args) throws Exception
	{
		if(args.length < 6){
			Util.pf( "you need to pass date (as 2012-05-29) and the number of look back date  and lookforward and path for config and ip and db type\n");
			System.exit(1);
		}
		String initdate = args[0];
		int lookback = Integer.parseInt(args[1]);
		int lookfw = Integer.parseInt(args[2]);
		String configpath = args[3];
		String ip = args[4];
		String dbtype = args[5];
		AggregationWrapper aw = new AggregationWrapper(dbtype, ip, "fastetl", "pixel_dimensions", "pixel_general");
		//AggregationWrapper aw = new AggregationWrapper("thorin-internal.adnetik.com", "fastpixel", "pixel_dimensions", "pixel_general");
		aw.schedule(initdate,lookback, lookfw, configpath);
	}
	public void schedule(String initdate, int lookback,  int lookfw, String configpath) throws Exception{
		PixelReporter.init(db_type, machine, db,dest_table,info_table);
		
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
				PixelReporter pr = new PixelReporter(strdate, 1, configpath, machine, db, dest_table, info_table);
				//pr.wrapup();
				pastfuture.add(Calendar.DATE, -1);
			}
		}
		//AggregationEngine aggEng = new AggregationEngine(initdate, lookback, false, configpath);
		//aggEng.wrapup();
		
		if(lookfw > 0)
		{

			//while(true)
			//	{
			GregorianCalendar future = new GregorianCalendar();
			int year = Integer.parseInt(initdate.split("-")[0]);
			int month = Integer.parseInt(initdate.split("-")[1]);
			int day = Integer.parseInt(initdate.split("-")[2]);
			GregorianCalendar past = (GregorianCalendar)future.clone();
			future.add(Calendar.DATE, lookfw);
			past.set(year, month - 1, day);
			past.add(Calendar.DATE, -1*lookback);
			while(past.before(future)){
				GregorianCalendar now = new GregorianCalendar();
				GregorianCalendar calendar = new GregorianCalendar();
//				String strmonth = ((past.get(Calendar.MONTH)+ 1) < 10)?"0"+(past.get(Calendar.MONTH)+ 1):""+(past.get(Calendar.MONTH)+ 1);
//				String strday = (past.get(Calendar.DATE) < 10)?"0"+past.get(Calendar.DATE):""+past.get(Calendar.DATE);
//				String strdate = ""+ past.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
				String strmonth = ((now.get(Calendar.MONTH)+ 1) < 10)?"0"+(now.get(Calendar.MONTH)+ 1):""+(now.get(Calendar.MONTH)+ 1);
				String strday = (now.get(Calendar.DATE) < 10)?"0"+now.get(Calendar.DATE):""+now.get(Calendar.DATE);
				String strdate = ""+ now.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
				Util.pf("processing date : %s\n" ,strdate);
				String yesterday = TimeUtil.getYesterdayCode();
				//int lb = FastUtil.CompareDates(TimeUtil.dayCode2Cal(strdate), TimeUtil.dayCode2Cal(TimeUtil.getTodayCode()));
				PixelReporter pr = new PixelReporter(strdate, 1, configpath, machine, db, dest_table, info_table);
				pr = new PixelReporter(yesterday, 1, configpath, machine, db, dest_table, info_table);
				//String today = TimeUtil.getTodayCode();
				//if(today.equals(strdate)){
				Thread.sleep(900000);
				//}
				past.add(Calendar.DATE, 1);
				if(past.after(calendar))
				{
					past = (GregorianCalendar)calendar.clone();
					future.add(Calendar.DATE, 1);
				}
				}

		}

		PixelReporter.wrapup();
		
		
		
		/*GregorianCalendar future = new GregorianCalendar();
		int year = Integer.parseInt(initdate.split("-")[0]);
		int month = Integer.parseInt(initdate.split("-")[1]);
		int day = Integer.parseInt(initdate.split("-")[2]);
		future.clear();
		future.set(year, month-1  , day);
		GregorianCalendar past = (GregorianCalendar)future.clone();
		past.add(Calendar.DATE, -1*lookback);
		while(future.after(past)){
			String strmonth = ((future.get(Calendar.MONTH)+ 1) < 10)?"0"+(future.get(Calendar.MONTH)+ 1):""+(future.get(Calendar.MONTH)+ 1);
			String strday = (future.get(Calendar.DATE) < 10)?"0"+future.get(Calendar.DATE):""+future.get(Calendar.DATE);
			String strdate = ""+ past.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
			Util.pf("processing date : %s\n" ,strdate);
			PixelReporter pr = new PixelReporter(strdate, 1, configpath, "thorin-internal.adnetik.com", "pixel", "pixel_fast_general", "pixel_dimensions");
			future.add(Calendar.DATE, -1);
		}
		Util.pf("Done with everything.\n");*/
		

	}

}
