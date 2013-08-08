
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

public class BudgetLineQuery extends AbstractMapper.LineFilter
{		
	public static final int BUDGET_CAMP_ID = 652;
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
		if(ble == null)
			{ return null; }
		
		try {
			String wtpid = ble.getField("wtp_user_id").trim();
			String tstamp = ble.getField("date_time").trim();
			int lineid = ble.getIntField("line_item_id");
			
			int campid = ble.getIntField("campaign_id");
			
			// lineid = 1887949237
			
			if(campid == 652 && wtpid.length() > 0)
			{
				String resval = Util.sprintf("%d\t%s", lineid, tstamp);
				return new String[] { wtpid, resval };		
			}			
		} catch (Exception ex) {
			
			return null;
			
		}
		
		return null;
	}
}
