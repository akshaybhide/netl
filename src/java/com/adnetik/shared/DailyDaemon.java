
package com.adnetik.shared;

import java.util.*;
import java.io.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

public abstract class DailyDaemon
{		
	// Five minutes
	private static final long SLEEP_INC = 5*60*1000;	
	
	private String _curDayCode;
	
	public DailyDaemon(String dc)
	{
		Util.massert(TimeUtil.checkDayCode(dc), "Invalid day code %s", dc);
		
		_curDayCode = dc;
	}
	
	protected abstract String getShortStartTimeStamp();
	
	protected abstract void runProcess();
	
	private String getStartTimeStamp()
	{
		String shortts = getShortStartTimeStamp();
		return Util.sprintf("%s %s", _curDayCode, shortts);			
	}
	
	// This is the "standard" way most clients should get the day to run for
	public String getPrevDayCode()
	{
		return TimeUtil.dayBefore(_curDayCode);	
	}
	
	public void startDaemon()
	{
		for(int i = 0; i < 10000; i++)
		{	
			String startts = getStartTimeStamp();
			
			// Wait until start time for next day
			while(true)
			{
				String curts = Util.cal2LongDayCode(new GregorianCalendar());
				
				if(curts.compareTo(startts) > 0)
				{
					Util.pf("Time is %s, starting for %s\n", curts, _curDayCode);
					break; 
				} 
				
				Util.pf("z");
				try { Thread.sleep(SLEEP_INC); }
				catch (InterruptedException inex) {
					throw new RuntimeException(inex);	
				}
			}
			
			// Ready to go			
			Util.pf("DailyDaemon::Starting process for yest=%s\n", getPrevDayCode());
			runProcess();			
			Util.pf("DailyDaemon::Finished process for yest=%s\n", getPrevDayCode());
			
			
			// Increment day code
			_curDayCode = TimeUtil.dayAfter(_curDayCode);
		}		
	}
}
