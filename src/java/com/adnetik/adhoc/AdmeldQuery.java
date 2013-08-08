
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

public class AdmeldQuery extends AbstractMapper.LineFilter
{		

	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v14, line);
		if(ble == null)
		{ 
			Util.pf("\nFound error field");
			return null; 
		}

		int campid = ble.getIntField(LogField.campaign_id);
		String adex = ble.getField(LogField.ad_exchange);
		Util.massert("admeld".equals(adex));
		
		
		if(campid == 1306)
		{
			String wprice = ble.getField(LogField.winner_price);
			String bid = ble.getField(LogField.bid);
			String datetime = ble.getField(LogField.date_time);
			
			return new String[] { datetime, Util.sprintf("%s\t%s", wprice, bid) };
		}
		
		return null;
	}
}
