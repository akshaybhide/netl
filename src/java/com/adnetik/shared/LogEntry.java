
package com.adnetik.shared;

import java.util.*;

import com.adnetik.shared.Util.*;
 
public abstract class  LogEntry
{	
	public static List<String> LOGTYPE_PLUS_PIXEL = Util.vector();
	
	static {
		for(LogType ltype : LogType.values())
			{  LOGTYPE_PLUS_PIXEL.add(ltype.toString()); }
		
		LOGTYPE_PLUS_PIXEL.add("pixel");
	}
	
	public boolean hasField(String fname)
	{
		return hasField(LogField.valueOf(fname));	
	}
	
	@Deprecated
	public final String getField(String fname)
	{
		LogField lfield = LogField.valueOf(fname);
		return getField(lfield);		
	}
	
	public abstract String getField(LogField fname);
	
	public abstract boolean hasField(LogField fname);
	
	@Deprecated
	public Integer getIntField(String fname)
	{
		String s = getField(fname);
		
		if(s.length() == 0)
			{ return null; }
		
		return Integer.valueOf(s);
	}
	
	public Integer getIntField(LogField fname)
	{
		String s = getField(fname);
		
		if(s.length() == 0)
			{ return null; }
		
		return Integer.valueOf(s);
	}	
	
	@Deprecated
	public Double getDblField(String fname)
	{
		String s = getField(fname);
		
		if(s.length() == 0)
			{ return null; }
		
		return Double.valueOf(s);		
	}
	
	public Double getDblField(LogField fname)
	{
		String s = getField(fname);
		
		if(s.length() == 0)
			{ return null; }
		
		return Double.valueOf(s);		
	}	
	
	public String getDerivedField(LogField fname)
	{
		if(fname == LogField.hour)
		{
			String dt = getField(LogField.date_time).trim();
			
			
			// eg 2010-02-15 14:55:41 
			// return date_time.split(" ")[1].split(":")[0];	
			// Believe this stringbuilder version is faster
			
			StringBuilder sb = new StringBuilder();
			sb.append(dt.charAt(11));
			sb.append(dt.charAt(12));
			return sb.toString();
			
		} else if (fname == LogField.country_region) {
			
			StringBuilder sb = new StringBuilder();
			sb.append(getField(LogField.user_country));
			sb.append("__");
			sb.append(getField(LogField.user_region));
			return sb.toString();
			
		} else {
			
			throw new RuntimeException("Unrecognized virtual field name " + fname);	
		}
	}
	
}
