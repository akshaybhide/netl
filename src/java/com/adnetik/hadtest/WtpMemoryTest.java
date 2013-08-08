/**
 * 
 */
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.shared.BidLogEntry.*;


public class WtpMemoryTest
{
	//Map<WtpId, Set<String>> pixLookupMap = Util.treemap();

	// static Set<WtpId> testSet = Util.treeset();	
	static Set<WtpId> testSet = new HashSet<WtpId>(100000000);
	
	public static void main(String[] args) throws Exception
	{
		Random jrand = new Random();
		
		while(true)
		{
			WtpId wid = WtpId.randomId(jrand);
			
			if(!testSet.contains(wid))
			{
				testSet.add(wid);
				int tsize = testSet.size();
				
				if((tsize % 100000) == 0)
				{
					Util.pf("\nSize is %s\n", Util.commify(tsize));
					Util.showMemoryInfo();
				}				
			}
		}
	}
}
