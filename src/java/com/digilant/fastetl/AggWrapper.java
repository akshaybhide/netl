package com.digilant.fastetl;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.adnetik.shared.TimeUtil;
import com.adnetik.shared.Util;
public class AggWrapper {
	/*public static void main(String[] args) throws Exception
	{
		if(args.length < 4){
			Util.pf( "you need to pass date (as 2012-05-29) and the number of look back date  and lookforward and path for config\n");
			System.exit(1);
		}
		String initdate = args[0];
		int lookback = Integer.parseInt(args[1]);
		String lookfw = args[2];
		String configpath = args[3];
		AggregationEngine aggEng = new AggregationEngine(initdate, lookback, configpath);
		aggEng.flushToStaging();
		aggEng.checkLockMove();
		String latestdate = initdate;
		GregorianCalendar calendar = new GregorianCalendar();
		GregorianCalendar future = (GregorianCalendar) calendar.clone(); 
		future.add(Calendar.DATE, Integer.parseInt(lookfw));
		while(calendar.before(future)){
			String month = ((calendar.get(Calendar.MONTH)+ 1) < 10)?"0"+(calendar.get(Calendar.MONTH)+ 1):""+(calendar.get(Calendar.MONTH)+ 1);
			String day = (calendar.get(Calendar.DATE) < 10)?"0"+calendar.get(Calendar.DATE):""+calendar.get(Calendar.DATE);
			String date = ""+ calendar.get(Calendar.YEAR) +"-" + month +"-" + day;
			Util.pf("processing date : %s\n" ,date);
			int lb = FastUtil.CompareDates(TimeUtil.dayCode2Cal(date), TimeUtil.dayCode2Cal(latestdate));
			AggregationEngine newagg = new AggregationEngine(date, lb + 1, configpath);
			newagg.flushToStaging();
			newagg.checkLockMove();
			Thread.sleep(900000);
			latestdate = date;
			calendar = new GregorianCalendar();
		}
		Util.pf("Done with everything.\n");
		
	}*/
	public static void main(String[] args) throws Exception
	{
		if(args.length < 4){
			Util.pf( "you need to pass date (as 2012-05-29) and the number of look back date  and lookforward and path for config\n");
			System.exit(1);
		}
		String initdate = args[0];
		int lookback = Integer.parseInt(args[1]);
		int lookfw = Integer.parseInt(args[2]);
		String configpath = args[3];
		AggWrapper aw = new AggWrapper();
		aw.schedule(initdate,lookback, lookfw, configpath);
	}
	public void schedule(String initdate, int lookback, int lookfw, String configpath) throws Exception{
		
		// BACK FILL
		{
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
				AggregationEngine aggEng = new AggregationEngine(strdate, 1,configpath);
				aggEng.flushToStaging();
				aggEng.checkLockMove();
				pastfuture.add(Calendar.DATE, -1);
			}
		}
		//AggregationEngine aggEng = new AggregationEngine(initdate, lookback, false, configpath);
		//aggEng.wrapup();
		
		
		{
			
			GregorianCalendar future = new GregorianCalendar();
			int year = Integer.parseInt(initdate.split("-")[0]);
			int month = Integer.parseInt(initdate.split("-")[1]);
			int day = Integer.parseInt(initdate.split("-")[2]);
			GregorianCalendar past = (GregorianCalendar)future.clone();
			future.add(Calendar.DATE, lookfw);
			past.set(year, month - 1, day);
			//past.add(Calendar.DATE, -1*lookback);
			while(past.before(future)){
				GregorianCalendar calendar = new GregorianCalendar();
				String strmonth = ((past.get(Calendar.MONTH)+ 1) < 10)?"0"+(past.get(Calendar.MONTH)+ 1):""+(past.get(Calendar.MONTH)+ 1);
				String strday = (past.get(Calendar.DATE) < 10)?"0"+past.get(Calendar.DATE):""+past.get(Calendar.DATE);
				String strdate = ""+ past.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
				Util.pf("processing date : %s\n" ,strdate);
				//int lb = FastUtil.CompareDates(TimeUtil.dayCode2Cal(strdate), TimeUtil.dayCode2Cal(TimeUtil.getTodayCode()));
				AggregationEngine aggEng = new AggregationEngine(strdate, 1,configpath);
				aggEng.flushToStaging();
				aggEng.checkLockMove();
				int currhour = calendar.get(Calendar.HOUR_OF_DAY);
				int lasthourupdated = 0;
				if(strdate.equals(TimeUtil.getTodayCode())){
					Thread.sleep(900000);
				}
				past.add(Calendar.DATE, 1);
				if(past.after(calendar))
					past = (GregorianCalendar)calendar.clone();
			}

			
		}
	}

}
