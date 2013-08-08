
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

import com.adnetik.analytics.*;

public class EvoRoboClick extends AbstractMapper.LineFilter
{		
	public String[] filter(String line)
	{ 
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.click, line);
		if(ble == null)
			{ return null; }
		
		int campid = ble.getIntField("campaign_id");
		
		if(campid == 820)
		{
			String wtpid = ble.getField("wtp_user_id");
			String linetype = ble.getField("line_item_type");
			String lineid = ble.getField("line_item_id");
			String resval = Util.sprintf("%s\t%s", linetype, lineid);
			
			return new String[] { wtpid, resval };
		}
		
		return null;
	}
}
