
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;

public class ZipCodeCount extends AbstractMapper.LineFilter
{		
	public static String TARG_ZIP = "48105";
	
	public String[] filter(String line)
	{ 
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, LogVersion.v13, line);
		if(ble == null)
			{ return null; }			
		
		String zip = ble.getField(LogField.user_postal).trim();
		
		if(TARG_ZIP.equals(zip))
		{
			return new String[] { zip, "1" };	
		}
		
		return null;
	}
	
	// Subclasses override to modify path behavior
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		//pathset.addAll(HadoopUtil.getGlobPathList(conf, "/data/bid_all/2012-01-04/casale/*.log.gz"));
		pathset.addAll(HadoopUtil.getGlobPathList(conf, "/data/no_bid/2012-01-04/*/*.log.gz"));
		pathset.addAll(HadoopUtil.getGlobPathList(conf, "/data/bid_all/2012-01-04/*/*.log.gz"));
	}	
}
