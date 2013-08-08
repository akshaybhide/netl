
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

public class RetargCostAnalysis extends AbstractMapper.LineFilter
{			
	public String[] filter(String line)
	{ 
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v21, line);
		if(ble == null)
			{ return null; }			
		
		boolean isretarg = "retargeting".equals(ble.getField(LogField.line_item_type));
		WtpId wid = WtpId.getOrNull(ble.getField(LogField.wtp_user_id));
		
		if(isretarg && (wid != null))
		{
			Double bid = ble.getDblField(LogField.bid);
			Double wprice = Util.getWinnerPriceCpm(ble);
			String domain = ble.getField(LogField.domain);
			String res = Util.sprintf("%.04f\t%.04f\t%s", wprice, bid, domain);
			return new String[] { wid.toString(), res };
		}
		
		return null;
	}
	
	// Subclasses override to modify path behavior
	public void modifyPathSet(Configuration conf, Set<Path> pathset) throws IOException
	{
		
		pathset.addAll(HadoopUtil.getGlobPathList(conf, "/data/imp/*2013-03-04*.lzo"));
		
		// pathset.add(new Path("/data/imp/yahoo_2013-03-04_v21.lzo"));
	}	
}
