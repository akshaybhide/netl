
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

public class CheckReplace 
{
	public static void main(String[] args) throws Exception
	{	
		BufferedReader bread = FileUtils.getReader("ad_domain_6491652578861602495.txt");
		int lcount = 0;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String[] toks = oneline.split("\t");
			
			String domain = toks[1];
			
			if(domain.startsWith("adshost") && domain.indexOf("\\") > -1)
			{
				String newdom = domain.replaceAll("\\\\", "");
				
				Util.pf("Found domain=%s, newdom=%s, on line %d\n", domain, newdom, lcount);	
				
				
			}
			
			lcount++;
		}
		bread.close();
	}
}
