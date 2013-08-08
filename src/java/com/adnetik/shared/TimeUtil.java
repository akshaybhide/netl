
package com.adnetik.shared;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.regex.*;

import java.text.SimpleDateFormat;

import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.shared.Util.*;

public class TimeUtil
{
	
	private static Map<String, Integer> _DAYAGOMAP; 	
	
	public static final String DAY_SET_PATH = "/com/adnetik/resources/20YearDays.txt";
	
	private static SimpleDateFormat LONG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("HH:mm:ss");
	
	private static SimpleDateFormat NFS_PATH_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");

	private static TreeSet<String> _DAY_SET = getBigDaySet();
	
	// Start and end of summer
	public static final String SUMMER_START = "05-20";
	public static final String SUMMER_END = "09-21";
	
	// Length of a day in milliseconds
	public static final Long DAY_MILLI = 24*60*60*1000L;	

	public static int secondsPastMidnight(String tstamp)
	{
		String[] h_m_s = tstamp.split(":");
		return Integer.valueOf(h_m_s[0])*3600 + Integer.valueOf(h_m_s[1])*60 + Integer.valueOf(h_m_s[2]);
	}
	
	
	public static List<String> getDateRange(String alpha)
	{
		return getDateRange(alpha, getYesterdayCode());	
	}	
	
	public static List<String> getDateRange(String alpha, String omega)
	{
		Util.massert(alpha.compareTo(omega) < 0, "Day code %s is after code %s", alpha, omega);
		
		List<String> dlist = Util.vector();

		long curmilli = dayCode2Cal(alpha).getTimeInMillis();

		while(true)
		{
			Calendar c = new GregorianCalendar();
			c.setTimeInMillis(curmilli);
			String dc = cal2DayCode(c);
			dlist.add(dc);
			
			if(omega.equals(dc))
				{ break; }
			
			curmilli += Util.DAY_MILLI;
		}
		
		return dlist;
	}
	
	public static String getYesterdayCode()
	{
		return cal2DayCode(getYesterday());
	}
	
	public static String getTodayCode()
	{
		return cal2DayCode(getToday());
	}
	
	public static Calendar getToday()
	{
		Calendar cal = new GregorianCalendar();
		return cal;		
	}
	
	public static Calendar getYesterday()
	{
		Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(cal.getTimeInMillis() - DAY_MILLI);
		return cal;
	}
	
	public static Calendar dayBefore(Calendar cal)
	{
		Calendar dbef = new GregorianCalendar();
		dbef.setTimeInMillis(cal.getTimeInMillis() - DAY_MILLI);
		return dbef;		
	}
	
	public static boolean isDayOfWeek(ShortDowCode dow)
	{
		return isDayOfWeek(dow, getTodayCode());
	}
	
	public static boolean isDayOfWeek(ShortDowCode dow, String daycode)
	{
		Calendar cal = dayCode2Cal(daycode);
		int dowint = cal.get(Calendar.DAY_OF_WEEK);
		return (dowint == getDowCalCode(dow));
	}
	
	public static ShortDowCode getDowCode(Calendar cal)
	{
		int dowint = cal.get(Calendar.DAY_OF_WEEK);
		
		for(ShortDowCode sdc : ShortDowCode.values())
		{
			if(dowint == getDowCalCode(sdc))
				{ return sdc; }
		}
		
		throw new RuntimeException("Could not find dow code for calendar " + cal);
	}
	
	// Return map of daycode::NumDaysBefore
	public static SortedMap<String, Integer> getDaysAgoMap(int ndays, String daycode)
	{
		Util.massert(checkDayCode(daycode), "Invalid daycode %s", daycode);
		SortedMap<String, Integer> daymap = Util.treemap();
		String gimp = daycode;
		
		while(daymap.size() < ndays)
		{
			daymap.put(gimp, daymap.size());
			gimp = dayBefore(gimp);
		}
		return daymap;		
	}
	
	public static ShortDowCode getDowCode(String daycode)
	{
		return getDowCode(dayCode2Cal(daycode));
	}
	
	public static int getDowCalCode(ShortDowCode dow)
	{
		switch (dow)
		{
		case mon:
			return Calendar.MONDAY;
		case tue:
			return Calendar.TUESDAY;
		case wed:
			return Calendar.WEDNESDAY;
		case thr:
			return Calendar.THURSDAY;
		case fri:
			return Calendar.FRIDAY;
		case sat:
			return Calendar.SATURDAY;
		case sun:
			return Calendar.SUNDAY;
		default: 
			throw new RuntimeException("Should never happen");			
		}
	}
	
	public static String dayBefore(String oneday)
	{ return nDaysDelta(oneday, 1, true); }
	
	public static String nDaysBefore(String oneday, int N)
	{ return nDaysDelta(oneday, N, true); }

	public static String dayAfter(String oneday)
	{ return nDaysDelta(oneday, 1, false); }
	
	public static String nDaysAfter(String oneday, int N)
	{ return nDaysDelta(oneday, N, false); }
	
	private static String nDaysDelta(String oneday, int N, boolean isbefore)
	{
		String gimp = oneday;
		synchronized (_DAY_SET) {
			for(int i = 0; i < N; i++)
			{
				gimp = (isbefore ? _DAY_SET.lower(gimp) : _DAY_SET.higher(gimp));
				if(gimp == null)
					{ throw new RuntimeException("Day is out of range: " + gimp); }
			}
		}
		return gimp;
	}
	

	public static Calendar dayAfter(Calendar cal)
	{
		Calendar daft = new GregorianCalendar();
		daft.setTimeInMillis(cal.getTimeInMillis() + DAY_MILLI);
		return daft;		
	}	
	
	public static int dayCode2Int(String daycode)
	{
		String nodash = daycode.replace("-", "");
		return Integer.valueOf(nodash);
	}
	
	public static Calendar fromMilli(long millis)
	{
		Calendar cal = new GregorianCalendar();
		cal.setTimeInMillis(millis);
		return cal;		
	}
	

	public static Calendar dayCode2Cal(String daycode)
	{
		try {
			String[] toks = daycode.split("-");
			
			Util.massert(toks.length == 3);
			
			int year = Integer.valueOf(toks[0]);
			Util.massert(2000 <= year && year < 2100); // should be wide enough...
			
			int mnth = Integer.valueOf(toks[1]);
			Util.massert(1 <= mnth && mnth <= 12);
			
			int dayc = Integer.valueOf(toks[2]);
			Util.massert(1 <= dayc && dayc <= 31);
			
			Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("EST"));
			cal.set(Calendar.YEAR, year);
			cal.set(Calendar.MONTH, mnth-1); // Month is 0-indexed!!
			cal.set(Calendar.DAY_OF_MONTH, dayc);
			
			
			return cal;
			
		} catch (Exception ex) {
			return null;
		}
	}	
	
	public static boolean checkDayCode(String daycode)
	{
		boolean okay;
		synchronized (_DAY_SET) { okay = _DAY_SET.contains(daycode); }
		return okay;
	}	

	public static List<String> getMonthList(int year, int month)
	{
		List<String> monthlist = Util.vector();		
		String pref = Util.sprintf("%d-%s", year, Util.padLeadingZeros(month, 2));
			
		for(String onedate = pref+"-01"; onedate.startsWith(pref); onedate = dayAfter(onedate))
		{
			monthlist.add(onedate);
		}
		
		return monthlist;		
	}
	
	
	public static void assertValidDayCode(String daycode)
	{
		Util.massert(checkDayCode(daycode), "Invalid daycode %s", daycode);	
	}
	
	public static String longDayCodeNow()
	{
		return cal2LongDayCode(new GregorianCalendar());	
	}
	
	public static int daysAgo(String dayCode)
	{
		if(_DAYAGOMAP == null)
		{
			_DAYAGOMAP = Util.treemap();
			Calendar cal = new GregorianCalendar();
			
			for(int i = 0; i < 10000; i++)
			{
				_DAYAGOMAP.put(cal2DayCode(cal), i);
				cal.setTimeInMillis(cal.getTimeInMillis() - DAY_MILLI);
			}
		}
		
		return _DAYAGOMAP.get(dayCode);		
	}
	
	public static boolean isSummer(String daycode)
	{
		String monthday = daycode.substring(5);
		return (SUMMER_START.compareTo(monthday) < 0 && monthday.compareTo(SUMMER_END) < 0);
	}


	public static String cal2DayCode(Calendar cal)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		return sdf.format(cal.getTime());
	}	
	
	// TODO: make these objects ... shared? Thread-safety????
	public static String cal2LongDayCode(Calendar cal)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(cal.getTime());
	}		
	
	public static String cal2TimeStamp(Calendar cal)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		return sdf.format(cal.getTime());		
	}	
			
	private static Calendar tstamp2Cal(String timestamp, SimpleDateFormat dform)
	{
		try {
			synchronized (dform) 
			{
				Date d = dform.parse(timestamp);
				Calendar cal = new GregorianCalendar();
				cal.setTime(d);
				return cal;
			}
		} catch(java.text.ParseException pex) {
			throw new RuntimeException(pex);
		}		
	}
	
	public static List<String> getDateRange(int numdays)
	{
		return getDateRange(getYesterdayCode(), numdays);
	}	
	
	public static List<String> getDateRange(String omega, int numdays)
	{
		Set<String> sortme = Util.treeset();
		
		Calendar cal = dayCode2Cal(omega);
		
		while(sortme.size() < numdays)
		{
			sortme.add(cal2DayCode(cal));
			cal.setTimeInMillis(cal.getTimeInMillis() - DAY_MILLI);
		}
		
		return new Vector<String>(sortme);
	}	
	
	public static String getEstCompletedTime(int numcomplete, int numtotal, double startmillis)
	{	
		double timespent = Util.curtime() - startmillis;
		double factor = ((double) numtotal)/numcomplete;
		
		double est_time_millis = timespent * factor;
		
		Calendar calnow = new GregorianCalendar();
		calnow.add(Calendar.MILLISECOND, (int) Math.round(est_time_millis));
		return cal2LongDayCode(calnow);
	}
	
	
	public static Calendar longDayCode2Cal(String timestamp) 
	{
		return tstamp2Cal(timestamp, LONG_DATE_FORMAT);
	}	
	
	public static Calendar calFromNfsPath(String onepath)
	{
		String fname = (new File(onepath)).getName();
		String timecode = fname.split("\\.")[0];		
		return tstamp2Cal(timecode, NFS_PATH_DATE_FORMAT);
	}		
	
	private static TreeSet<String> getBigDaySet()
	{
		TreeSet<String> dayset = Util.treeset();

		InputStream resource = (new TimeUtil()).getClass().getResourceAsStream(DAY_SET_PATH);
		Scanner sc = new Scanner(resource, "UTF-8");
		
		while(sc.hasNextLine())
		{
			String s = sc.nextLine().trim();
			if(s.length() > 0)
				{ dayset.add(s.trim()); }
		}
		sc.close();
		
		// In 20 years starting from 2000-01-01, ending 2020-12-31, there are 7305 days
		Util.massert(dayset.size() == 7305);
		
		return dayset;
	}
		
	public static boolean isAbeforeB(String a, String b)
	{
		return a.compareTo(b) < 0;
	}
	
	public static void main(String[] args) throws IOException
	{
		TreeSet<String> dayrange = Util.treeset();
			
		dayrange.addAll(getDateRange("2012-06-29", "2012-10-05"));
		
		Util.pf("First day is %s, last is %s, size is %d", dayrange.first(), dayrange.last(), dayrange.size());
	}
}

