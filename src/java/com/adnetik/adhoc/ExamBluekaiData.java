
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.data_management.*;
import com.adnetik.data_management.BluekaiDataMan.*;

public class ExamBluekaiData
{
	
	public static void main(String[] args) throws IOException
	{
		Util.pf("Hello from ExamBluekaiData\n");
		
		String daycutoff = TimeUtil.nDaysBefore(TimeUtil.getYesterdayCode(), 30);
		
		BluekaiDataMan.setSingQ("2012-12-13");
		
		int usertotal = 100000;
		int dropuser = 0;
		
		int multidate = 0;
		int widedate = 0;

		for(int i = 0; i < usertotal; i++)
		{
			BluserPack bpack = BluekaiDataMan.getSingQ().nextPack();
			
			SortedSet<String> daycodeset = new TreeSet<String>(bpack.getSegDataMap().values());
					
			String lastupdate = daycodeset.last();
			
			boolean isdrop = false;
			
			if(daycutoff.compareTo(lastupdate) > 0)
			{
				// Util.pf("Going to drop user %d is ID %s, first date is %s, last is %s, cutoff is %s\n", 
				//	i, bpack.getWtpId(), daycodeset.first(), daycodeset.last(), daycutoff);
				
				dropuser++;
				isdrop = true;
			}
			
			if(!isdrop && daycodeset.size() > 1)
			{
				multidate++;
				
				String widecutoff = TimeUtil.nDaysBefore(daycodeset.last(), 15);
				if(widecutoff.compareTo(daycodeset.first()) > 0)
				{
					widedate++;
					
					Util.pf("User has wide date range: first/last is %s/%s, cutoff is %s\n", 
						daycodeset.first(), daycodeset.last(), widecutoff);
				}				
			}
			

			
		}
		
		Util.pf("Going to drop %d users out of %d\n", dropuser, usertotal);
		Util.pf("Found %d wide-date users, %d multi-date users\n", widedate, multidate);
		
	}
}
