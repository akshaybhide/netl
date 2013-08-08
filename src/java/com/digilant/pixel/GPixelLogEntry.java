package com.digilant.pixel;

import java.sql.SQLException;
import java.util.*;

import javax.sql.rowset.CachedRowSet;

import com.adnetik.shared.LogEntry;
import com.adnetik.shared.Util;
import com.digilant.mobile.DBConnection;

import com.adnetik.shared.Util.LogField;


public class GPixelLogEntry // extends LogEntry
{
	String[] toks;
	
	PixLogVersion vers;
	
	private static Map<PixLogVersion, Map<String, Integer>> _VFIELDMAP;
	
	public enum PixLogVersion { v1, v2 };
	public static void init(String machine, String db, String[] tables){
		_VFIELDMAP = new HashMap<PixLogVersion, Map<String, Integer>>();
		for(PixLogVersion v : PixLogVersion.values()){
			
			try {
					String query = "select * from " + tables[v.ordinal()];
					CachedRowSet rs = DBConnection.runQuery(machine, db, query);
					Map<String, Integer> fmap = Util.treemap();
					while(rs.next()){
					String colname = rs.getString(1).trim();
					int colno = rs.getInt(2);
					fmap.put(colname, colno);
				}
					_VFIELDMAP.put(v, fmap);
				
			} catch (SQLException sqlex) {
				
				throw new RuntimeException(sqlex);
			}			
			

		}
	}
	
	public GPixelLogEntry(String line, String filename)
	{
		toks = line.split("\t");
		vers = getVersion(filename);
	}
	public static PixLogVersion getVersion(String filename){
		int startidx = filename.indexOf("pixel_")+6;
		int endidx = startidx + 2;
		String version = (String) filename.subSequence(startidx, endidx);
		return PixLogVersion.valueOf(version);
	}

	public static GPixelLogEntry getOrNull(String line, String filename)
	{
		try {
			GPixelLogEntry ple = new GPixelLogEntry(line, filename); 
			//ple.check();
			return ple;
			
		} catch (Exception ex) {
			return null;
		}
	}
	
	void check()
	{
		// This tests that the field is populated, and in integer format
		int pixid = Integer.valueOf(getField("pixel_id"));		
		// int pixid = getIntField(LogField.pixel_id);
	}

	public String getField(String fname)
	{
		int fi = getFieldIndex(vers, fname);
		
		if(fi >= toks.length)
			{ throw new FormatException(Util.sprintf("Requested column %d but only %d columns available", fi, toks.length)); }
		
		return toks[fi];
	}
	
	public static boolean hasField(LogField lfied)
	{
		Util.massert(false, "Not implemented");
		return true;
	}
	
	public static int getFieldIndex(PixLogVersion v, String fname)
	{
		if(_VFIELDMAP == null)
		{
			init("thorin-internal.digilant.com","metadata",new String[]{"v1","v2"});
		}
		
		if(!_VFIELDMAP.get(v).containsKey(fname))
		{
			throw new RuntimeException("Unknown field name: " + fname);	
		}
		
		return _VFIELDMAP.get(v).get(fname);
	}
	
	/*private static void initVFieldMap()
	{
		_VFIELDMAP = Util.treemap();
		addFieldNames(PixLogVersion.v1, getV1Fields());
	}
	*/
/*	private static void addFieldNames(PixLogVersion v, List<String> fnames)
	{
		Map<String, Integer> fmap = Util.treemap();
		
		for(String fn : fnames)
			{ fmap.put(fn, fmap.size()); }
		
		_VFIELDMAP.put(v, fmap);
	}
	*/
	/*private static List<String> getV1Fields()
	{
		List<String> flist = Util.vector();
		flist.add("date_time");
		flist.add("ip");
		flist.add("country");
		flist.add("region");
		flist.add("request_uri");
		flist.add("referer");
		flist.add("useragent");
		flist.add("wtp_user_id");
		flist.add("pixel_id");
		flist.add("pixel_type");
		flist.add("pixel_format");
		flist.add("secure"); // This field is probably misnamed
		
		return flist;
	}
	*/
	public static class FormatException extends RuntimeException
	{
		public FormatException(String m) { super(m); }
	}
	
	//public int getIntField

}
