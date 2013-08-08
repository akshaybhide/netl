
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

public class RetargSuspicion extends AbstractMapper.LineFilter
{		

	public String[] filter(String line)
	{
		String[] toks = line.split("\t");
		
		if(toks.length > 20)
		{
			// Bid Log Entry
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v14, line);
			if(ble == null)
				{  return null; }

			
			int lineid = ble.getIntField(LogField.line_item_id);
			
			if(lineid == 1887955045)
			{
				String wtpid = ble.getField(LogField.wtp_user_id).trim();
				wtpid = (wtpid.length() == 0 ? "NOTSET" : wtpid);
				return new String[] { wtpid, "imp" };
			}
			
		} else {
			
			PixelLogEntry ple = PixelLogEntry.getOrNull(line);
			if(ple == null)
				{ return null; }
			
			int pixid = ple.getIntField(LogField.pixel_id);
			
			if(pixid == 5225)
			{
				String wtpid = ple.getField(LogField.wtp_user_id);
				wtpid = (wtpid.length() == 0 ? "NOTSET" : wtpid);
				return new String[] { wtpid, "pix" };
			}
		}
		
		return null;
	}
	
	// Subclasses override to modify path behavior
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		List<String> slist = Util.vector();
		slist.add("/data/imp/*_2012-02-05.lzo");
		slist.add("/data/pixel/pix_2012-02-05.lzo");
		slist.add("/data/pixel/pix_2012-02-04.lzo");
		
		for(String s : slist)
		{
			List<Path> pathlist = HadoopUtil.getGlobPathList(conf, s);
			pathset.addAll(pathlist);
		}
		
		//Util.pf("\nPathset is %s, size is %d", pathset, pathset.size());
		//System.exit(1);
	}	
	
}
