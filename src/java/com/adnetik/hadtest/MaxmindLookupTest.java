
package com.adnetik.hadtest;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;


// This is a test of the speed and accuracy of lookups into the MaxMind database. 
public class MaxmindLookupTest
{

	public static void main(String[] args) throws Exception
	{
		Scanner sc = new Scanner(new File("ip_test_file.txt"));
		int hits = 0;
		int lookups = 0;
		
		
		while(sc.hasNextLine())
		{
			String ip = sc.nextLine().trim();
			
			//SortedMap<Integer, String> catinfo = Maxmind.lookupCat(ip);
			SortedMap<Integer, String> catinfo = Util.treemap();
			
			Util.pf("\nIP address=%s, corresponds to %d longform", ip, Util.ip2long(ip));
			
			if(catinfo != null)
			{
				hits++;
				int catid = catinfo.firstKey();
				Util.pf("\n\tCategory=%d, corpname=%s", catid, catinfo.get(catid)); 
			}
			
			lookups++;
		}
			
		Util.pf("\nFound %d hits out of %d lookups", hits, lookups);
	}
}
