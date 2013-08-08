
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

public class SpillOverQuery extends AbstractMapper.LineFilter
{		
	final String TARG_DAYCODE = "2012-03-15";
	

	public String[] filter(String line)
	{
		String[] toks = line.split("\t");
		
		// Bid Log Entry
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v14, line);
		if(ble == null)
			{  return null; }
		
		String timestamp = ble.getField(LogField.date_time);
		String dateonly = timestamp.substring(0, 10);
		
		return new String[] { dateonly, "1" };
	}
	
	// Subclasses override to modify path behavior
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		String pattern = Util.sprintf("/data/imp/*%s.lzo", TARG_DAYCODE);
		List<Path> pathlist = HadoopUtil.getGlobPathList(conf, pattern);
		pathset.addAll(pathlist);
		
		//Util.pf("\nPathset is %s, size is %d", pathset, pathset.size());
		//System.exit(1);
	}	
	
}
