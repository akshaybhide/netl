/**
 * 
 */
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place
import com.adnetik.data_management.*; // Put all the enums in the same place


public class PairTypeTest
{
	public static void main(String[] args) throws Exception
	{
		Map<Pair<ExcName, LogType>, Integer> exctypemap = LogSync.getExcTypeMap();
		
		for(Pair<ExcName, LogType> elpair : exctypemap.keySet())
		{
			Util.pf("EL pair is %s\n", elpair);	
			
		}
		
		// exctypemap.put(Pair.build(LogType.imp, ExcName.rtb), 45);
		
		
		Pair<ExcName, LogType> badkey = Pair.build(ExcName.rtb, LogType.imp);
		Integer realbadkey = 3;
		// Integer failid = exctypemap.get(realbadkey);	
		
		// Map<String, Integer> simplemap = Util.treemap();
		// simplemap.put("danb", 5);
		// Integer newid = simplemap.get(realbadkey);
		// Integer newid = Util.safeget(simplemap, realbadkey);
		
		// simplemap.put(45, 4);
	}
	
}
