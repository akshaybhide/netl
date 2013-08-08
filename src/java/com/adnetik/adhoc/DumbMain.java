
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
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;
import com.adnetik.userindex.*;
import com.adnetik.userindex.StagingInfoManager.*;

public class DumbMain
{
	public static void main(String[] args)
	{
		String daycode = TimeUtil.getYesterdayCode();
		
		UniqCountReporter  ucr = new UniqCountReporter(daycode);
		ucr.generateFileMap();
		ucr.runUniqueCounts();
	}
}
