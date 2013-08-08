
package com.digilant.mobile;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;


import com.digilant.fastetl.FastUtil;
import com.adnetik.shared.TimeUtil;
import com.adnetik.shared.Util;

public class AggregationWrapper<T extends AggregationEngine> {
	public String machine;
	public String db;
	public static String dest_table;
	public String info_table;
	public static String mode = "";
	public static boolean alreadywrappedyesterday = false;
	
	public AggregationWrapper(String machine, String db, String info_table, String dest_table) throws SQLException{
		this.machine = machine;
		this.db = db;
		this.dest_table = dest_table;
		this.info_table = info_table;
		HAggregator.init(machine, db, dest_table, "mobile_hourly");
		GeneralDimension.init(machine,db,info_table);
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
		AggregationWrapper<AggregationEngine> aw = new AggregationWrapper<AggregationEngine>(ip, "mobile", "mobile_dimensions", "mobile_test");
		
		aw.schedule(initdate,lookback, lookfw, configpath);
	}
	public void schedule(String initdate, int lookback, int lookfw, String configpath) throws SQLException, IOException {
		
		// BACK FILL
		if(lookfw==0){
			//HAggregator.report(initdate, 23, 3, false);
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
				AggregationEngine aggEng = new AggregationEngine(machine, db, info_table, dest_table, strdate, 1, false, configpath);
				try {
					aggEng.wrapup();
					HAggregator.report(strdate, 23, 3, false);
					HAggregator.AddDate2DB(strdate);
					aggEng.cleanfraud(strdate);
					Util.pf("done with date : %s\n" ,strdate);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println(e.getMessage());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
				pastfuture.add(Calendar.DATE, -1);
			}
			GregorianCalendar clean = new GregorianCalendar();
			clean.clear();
			clean.set(year, month - 1, day);
			clean.add(Calendar.DATE, 1);
			String strmonth = ((clean.get(Calendar.MONTH)+ 1) < 10)?"0"+(clean.get(Calendar.MONTH)+ 1):""+(clean.get(Calendar.MONTH)+ 1);
			String strday = (clean.get(Calendar.DATE) < 10)?"0"+clean.get(Calendar.DATE):""+clean.get(Calendar.DATE);
			String strdate = ""+ clean.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;

			HAggregator.cleanup(strdate, lookback+1);
		}
		//AggregationEngine aggEng = new AggregationEngine(initdate, lookback, false, configpath);
		//aggEng.wrapup();
		//for Cron job
		//Future
		if(lookfw > 0){
			GregorianCalendar calendar = new GregorianCalendar();
			int currhour = calendar.get(Calendar.HOUR_OF_DAY);
			GregorianCalendar c = new GregorianCalendar();
			c.add(Calendar.DATE, -1);
			String strmonth = ((c.get(Calendar.MONTH)+ 1) < 10)?"0"+(c.get(Calendar.MONTH)+ 1):""+(c.get(Calendar.MONTH)+ 1);
			String strday = (c.get(Calendar.DATE) < 10)?"0"+(c.get(Calendar.DATE)):""+c.get(Calendar.DATE);
			String strdateyesterday = ""+ c.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
			alreadywrappedyesterday = HAggregator.isWrappedUp(strdateyesterday);
			if(currhour==2 && !alreadywrappedyesterday){
				AggregationEngine newagg = new AggregationEngine(machine, db, info_table, dest_table, strdateyesterday,   1, true, configpath);
				calendar = new GregorianCalendar();
				try {
					//newagg.wrapup();
					HAggregator.report(strdateyesterday, 23, 3, false);
					HAggregator.AddDate2DB(strdateyesterday);
					HAggregator.WrapUp2DB(strdateyesterday);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			int currq = calendar.get(Calendar.MINUTE)/15;
			strmonth = ((calendar.get(Calendar.MONTH)+ 1) < 10)?"0"+(calendar.get(Calendar.MONTH)+ 1):""+(calendar.get(Calendar.MONTH)+ 1);
			strday = (calendar.get(Calendar.DATE) < 10)?"0"+calendar.get(Calendar.DATE):""+calendar.get(Calendar.DATE);
			String strdate = ""+ calendar.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;
			Util.pf("processing date : %s\n" ,strdate);
			AggregationEngine newagg = new AggregationEngine(machine, db, info_table, dest_table, strdate,   1, true, configpath);
			try {
				newagg.wrapup();
				if(currq == 3)
					HAggregator.report(strdate, currhour, currq, true);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println(e.getMessage());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println(e.getMessage());
			}
			
		}
		Util.pf("Done with everything.\n");
		

	}

}
