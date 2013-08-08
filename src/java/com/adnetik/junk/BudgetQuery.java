
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

public class BudgetQuery extends AbstractMapper.LineFilter
{		
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.conversion, line);
		if(ble == null)
			{ return null; }
		
		String wtpid = ble.getField("wtp_user_id").trim();
		int campid = ble.getIntField("campaign_id");
		int lineid = ble.getIntField("line_item_id");
		 
		if(campid == BudgetLineQuery.BUDGET_CAMP_ID && wtpid.length() > 0)
		{
			return new String[] { wtpid, "" + lineid };		
		}
		
		return null;
	}

	/*
	public String[] filter(String line)
	{
		PixelLogEntry ple = new PixelLogEntry(line);

		String wtpid = ple.getField("wtp_user_id").trim();
		int pixelid = ple.getIntField("pixel_id");
		 
		if(pixelid == 3473 && wtpid.length() > 0)
		{
			return new String[] { wtpid, "" + pixelid };		
		}
		
		return null;
	}
	*/
}
