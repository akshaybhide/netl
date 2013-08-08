package com.adnetik.fastetl;

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.adnetik.shared.Util;

public class AggWrapper {
	public static void main(String[] args) throws Exception
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
			int lb = FastUtil.CompareDates(Util.dayCode2Cal(date), Util.dayCode2Cal(latestdate));
			AggregationEngine newagg = new AggregationEngine(date, lb + 1, configpath);
			newagg.flushToStaging();
			newagg.checkLockMove();
			Thread.sleep(900000);
			latestdate = date;
			calendar = new GregorianCalendar();
		}
		Util.pf("Done with everything.\n");
		
	}	

}
