
package com.adnetik.shared;

import java.util.*;
import java.io.*;

public class GoogleVertLookup
{
	// THis is a JAR file path
	public static final String VERT_CSV_PATH = "/com/adnetik/resources/verticals.tsv";
	
	private static GoogleVertLookup _SING;
	
	public Map<Integer, String>  baseCodeMap = Util.treemap();
	public Map<Integer, Integer> prntCodeMap = Util.treemap();
	
	
	public static void main(String[] args)
	{
		getSing();
	}
	
	public Map<Integer, String> getBaseCodeMap()
	{
		return Collections.unmodifiableMap(baseCodeMap);	
		
	}
	
	public static GoogleVertLookup getSing()
	{
		if(_SING == null)
			{ _SING = new GoogleVertLookup(); }
		
		return _SING;
	}
	
	
	public String getBaseCode(int baseId)
	{
		return baseCodeMap.get(baseId);	
		
	}
	
	public String getFullCode(int baseId)
	{
		if(!prntCodeMap.containsKey(baseId))
			{ throw new RuntimeException("Vertical ID not found: " + baseId); }
		
		int parCode = prntCodeMap.get(baseId);
		
		// code1 >> code2 >> code3	
		if(parCode == 0)
			{ return getBaseCode(baseId); }
		
		
		return getFullCode(parCode) + ">>" + getBaseCode(baseId);
	}
	
	GoogleVertLookup() 
	{
		try {
			InputStream resource = getClass().getResourceAsStream(VERT_CSV_PATH);
			InputStreamReader ireader = new InputStreamReader(resource, "UTF-8");
			
			Scanner sc = new Scanner(ireader);
			// Scanner sc = new Scanner(resource, BidLogEntry.BID_LOG_CHARSET);
			
			String head = sc.nextLine();
			Util.massert(head.indexOf("ID") > -1);
			
			while(sc.hasNextLine())
			{
				String s = sc.nextLine();	
				
				//System.out.printf("\nLine is %s", s);
				
				String[] toks = s.split("\t");
				Util.massertEq(toks.length, 3);
				
								
				String code = toks[0];
				int baseId = Integer.valueOf(toks[1]);
				int prntId = Integer.valueOf(toks[2]);
				
				baseCodeMap.put(baseId, code);
				prntCodeMap.put(baseId, prntId);
			}
			
			
		} catch (Exception ex) {
			
			throw new RuntimeException(ex);	
		}
		
	}
}
