
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

public class MegaUserDomain extends AbstractMapper.LineFilter
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
			String domain = ble.getField("domain").trim();
			String userip = ble.getField("user_ip").trim();
						
			String reskey = Util.sprintf("%s____%s", domain, userip);
			return new String[] { reskey, "1" };
		}
		
		return null;
		
	}
}
