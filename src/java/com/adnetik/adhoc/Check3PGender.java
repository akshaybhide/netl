
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

import com.adnetik.data_management.*;
import com.adnetik.data_management.BluekaiDataMan.*;
import com.adnetik.analytics.*;

public class Check3PGender 
{
	public static void main(String[] args) throws IOException
	{
		Util.pf("Hello, check gender.\n");
		
		BluekaiDataMan.setSingQ("2013-04-15");
		BlueUserQueue buq = BluekaiDataMan.getSingQ();
		
		int ftarg = 22599;
		int mtarg = 22598;
		
		int fcount = 0, mcount = 0, tcount = 0, bcount = 0;
		
		for(int i = 0; i < 100000; i++)
		{
			BluserPack bpack = buq.nextPack();			
			int numseg = bpack.getSegDataMap().size();
			
			if(numseg < 100)
				{ continue; }
			
			boolean ismle = bpack.hasSegmentEver(mtarg);
			boolean isfem = bpack.hasSegmentEver(ftarg);
			
			fcount += (isfem ? 1 : 0);
			mcount += (ismle ? 1 : 0);
			bcount += (isfem && ismle ? 1 : 0);
			tcount += 1;			
		}
		
		double fratio = fcount; 
		fratio /= tcount;
		
		double mratio = mcount;
		mratio /= tcount;
		
		double bratio = bcount;
		bratio /= tcount;
		
		Util.pf("Found %.03f male, %.03f female, %.03f both, %d total, %d BOTH\n",
			mratio, fratio, bratio, tcount, bcount);
		
	}
}
