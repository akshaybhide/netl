
package com.adnetik.shared;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import com.adnetik.shared.*;

public class SharedResource
{
	public static final String DEFAULT_TIMEZONE_PATH = "/com/adnetik/resources/DefaultTimeZoneInfo.txt";
	
	private static Map<String, Integer> _DEF_TIME_MAP;
	
	private static synchronized void popDefTimeMap()
	{
		if(_DEF_TIME_MAP != null)
			{ return; }
		
		_DEF_TIME_MAP = Util.conchashmap();
		InputStream resource = (new SharedResource()).getClass().getResourceAsStream(DEFAULT_TIMEZONE_PATH);
		
		Scanner sc = new Scanner(resource, "UTF-8");
		while(sc.hasNextLine())
		{
			// Util.pf("Found new default time zone\n");
			String s = sc.nextLine();
			String[] ckey_count_timz = s.split("\t");
			_DEF_TIME_MAP.put(ckey_count_timz[0], Integer.valueOf(ckey_count_timz[2]));
		}
		sc.close();		
		
	}
	
	public static Integer getDefaultTimezoneMinutes(String country, String region)
	{
		if(country.length() == 0)
			{ return null; }
		
		if(region.trim().length() == 0)
			{ region = "??"; }
		
		String combkey = country + Util.DUMB_SEP + region;
		
		if(_DEF_TIME_MAP == null)
			{ popDefTimeMap(); }
		
		return _DEF_TIME_MAP.get(combkey);
	}	
	
}
