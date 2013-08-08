
package com.adnetik.analytics;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.LogType;

// TODO: merge these things into one file
import com.adnetik.analytics.CountMapWrapper.LineGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.BarGraphWrapper;
import com.adnetik.analytics.EpsWrapperTool.HistogramWrapper;


// Scan the day's worth of impression data to create a 
// User Reach database, which is basically just a bunch of <line_item_id, wtp_user_id, timestamp> 
// Triples
public class AdexValueGapScan extends AbstractMapper.LineFilter
{		

	public String[] filter(String line)
	{
		//Util.pf("\nLine is %s", line);
		BidLogEntry ble;
		try {
			ble = new BidLogEntry(LogType.imp, line);
		} catch (BidLogEntry.BidLogFormatException blex) {
			
			throw new RuntimeException(blex);			
		}
	
		Double bid = ble.getDblField("bid");
		Double wprice = ble.getDblField("winner_price");

		// This should really be unnecessary!!!
		if(bid == null || wprice == null)
			{ return null; }

		if(bid < 1)
			{ return null; }


		
		String adex = ble.getField("ad_exchange");
		double cprice = Util.convertPrice(adex, wprice);
		double vgap = (bid - cprice);

		if(vgap < 0)
			{ vgap = 0; }
		
		String reskey = Util.sprintf("%s%s%.02f", adex, Util.DUMB_SEP, vgap);
		
		//Util.pf("\nReskey is %s", reskey);
		
		/*
		Util.pf("\nMAde it here -------");
		Util.pf("\n\tbid: %s", ble.getField("bid"));
		Util.pf("\n\twnp: %s", ble.getField("winner_price"));
		Util.pf("\n\tkey: %s", reskey);
		*/
		
		return new String[] { reskey, "1" };
	}
}
