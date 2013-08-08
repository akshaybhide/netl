
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

public class NetherlandQuery extends AbstractMapper.LineFilter
{		
	Set<String> targSet = Util.treeset();
	
	// TODO: this is really hokey
	@Override
	public void subConfigure(JobConf conf)  { 
		
		super.subConfigure(conf);

		try {
			List<String> idlist = HadoopUtil.readFileLinesE(FileSystem.get(conf), "/mnt/data/burfoot/netherland/idlist.txt");
			targSet.addAll(idlist);
			Util.pf("\nFound %d total ids, %d unique", idlist.size(), targSet.size());
		} catch (IOException ioex) {
				
			throw new RuntimeException(ioex);
		}
	}	
	
	public String[] filter(String line)
	{
		BidLogEntry ble = BidLogEntry.getOrNull(LogType.no_bid_all, line);
		if(ble == null)
			{ return null; }
		
		String wtpid = ble.getField("wtp_user_id").trim();

		if(wtpid.length() == 36 && targSet.contains(wtpid))
		{
			return new String[] { wtpid, "1" };
		}
		
		return null;
	}
}
