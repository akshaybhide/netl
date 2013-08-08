
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

// Rename the files named "id" to "improvedigital"
public class TestNewDerived
{		
	public static void main(String[] args) throws Exception
	{
		BufferedReader bread = FileUtils.getReader("testdata.txt");
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			BidLogEntry ble = new BidLogEntry(LogType.imp, LogVersion.v22, oneline);
			
			String methA = ble.getField(LogField.hour);
			String methB = newHourCalc(ble.getField(LogField.date_time));
			
			if(Math.random() < .001)
				{ Util.pf("METHA: %s, METHB: %s\n", methA, methB); }
			
			
			Util.massert(methA.equals(methB),
				"MethA gives %s, methB gives %s", methA, methB);
		}
		
		bread.close();
	}
	
	private static String newHourCalc(String datetime)
	{
		String dt = datetime.trim();
		
		Util.massert(dt.length() == 19, 
			"Expected string length 19, got %d", dt.length());
		
		StringBuilder sb = new StringBuilder();
		sb.append(dt.charAt(11));
		sb.append(dt.charAt(12));
		return sb.toString();
		
		// eg 2010-02-15 14:55:41		
		
		
	}
}
