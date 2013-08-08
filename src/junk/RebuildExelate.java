
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
import com.adnetik.data_management.*;

public class RebuildExelate 
{
	public static void main(String[] args) throws Exception
	{
		String daycode = args[0];
		
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode");
		
		ExelateDataMan.ExMergeOp exmo = new ExelateDataMan.ExMergeOp(daycode);
		exmo.runOp();
	}
}
