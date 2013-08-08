
package com.adnetik.shared;

import java.util.*;

import com.adnetik.shared.Util.*;

public class PixelLogEntry extends LogEntry
{
	String[] toks;
	
	PixLogVersion _logVers;
	
	private static Map<PixLogVersion, Map<LogField, Integer>> _VFIELDMAP;
	
	public enum PixLogVersion { v1 };
	
	
	public PixelLogEntry(PixLogVersion vers, String logline)
	{
		toks = logline.split("\t");
		_logVers = vers;
		
	}
	
	public PixelLogEntry(String line)
	{
		this(PixLogVersion.v1, line);
	}

	public static PixelLogEntry getOrNull(String line)
	{
		return getOrNull(PixLogVersion.v1, line);	
	}
	
	public static PixelLogEntry getOrNull(PixLogVersion vers, String line)
	{
		try {
			PixelLogEntry ple = new PixelLogEntry(vers, line); 
			ple.basicCheck();
			return ple;
			
		} catch (Exception ex) {
			return null;
		}
	}
	
	public String getLogLine()
	{
		return Util.join(toks, "\t");
	}
	
	
	public void basicCheck()
	{
		// This tests that the field is populated, and in integer format
		try { int pixid = getIntField(LogField.pixel_id); }
		catch (Exception ex) 
		{ throw new FormatException("Error converting pixel ID to integer");	 }
	}
	
	public boolean hasField(LogField fname)
	{
		return _VFIELDMAP.get(_logVers).containsKey(fname);
	}
	
	public String getField(LogField fname)
	{
		int fi = getFieldIndex(_logVers, fname);
		
		if(fi >= toks.length)
			{ throw new FormatException(Util.sprintf("Requested column %d but only %d columns available", fi, toks.length)); }
		
		return toks[fi];
	}
	
	public static int getFieldIndex(PixLogVersion v, LogField fname)
	{
		if(_VFIELDMAP == null)
		{
			initVFieldMap();
		}
		
		if(!_VFIELDMAP.get(v).containsKey(fname))
		{
			throw new RuntimeException("Unknown field name: " + fname);	
		}
		
		return _VFIELDMAP.get(v).get(fname);
	}
	
	private static void initVFieldMap()
	{
		_VFIELDMAP = Util.treemap();
		addFieldNames(PixLogVersion.v1, getV1Fields());
	}
	
	private static void addFieldNames(PixLogVersion v, List<LogField> fnames)
	{
		Map<LogField, Integer> fmap = Util.treemap();
		
		for(LogField fn : fnames)
			{ fmap.put(fn, fmap.size()); }
		
		_VFIELDMAP.put(v, fmap);
	}
	
	private static List<LogField> getV1Fields()
	{
		List<LogField> flist = Util.vector();
		flist.add(LogField.date_time);
		flist.add(LogField.user_ip);
		flist.add(LogField.user_country);
		flist.add(LogField.user_region);
		flist.add(LogField.request_uri);
		flist.add(LogField.referer);
		flist.add(LogField.useragent);
		flist.add(LogField.wtp_user_id);
		flist.add(LogField.pixel_id);
		flist.add(LogField.pixel_type);
		flist.add(LogField.pixel_format);
		flist.add(LogField.secure); // This field is probably misnamed
		
		return flist;
	}
	
	public static class FormatException extends RuntimeException
	{
		public FormatException(String m) { super(m); }
	}
	
	//public int getIntField

}
