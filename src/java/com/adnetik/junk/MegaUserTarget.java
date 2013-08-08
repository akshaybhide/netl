
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

public class MegaUserTarget extends AbstractMapper.LineFilter
{		

	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, line);
		if(ble == null)
		{ 
			Util.pf("\nFound error field");
			return null; 
		}

		String wtp_id = ble.getField("wtp_user_id").trim();
		
		if("73d0f3a9-ec92-4021-9c94-4d5b16633270".equals(wtp_id))
		{
			String camp_id = ble.getField("campaign_id").trim();
			String line_id = ble.getField("line_item_id").trim();
			String line_type = ble.getField("line_item_type").trim();
			
			if(line_type.length() == 0)
				{ line_type = "NotSet"; }
			
			String reskey = Util.sprintf("%s____%s____%s", camp_id, line_id, line_type);
			return new String[] { reskey, "1" };
		}
		
		return null;
		
	}
}
