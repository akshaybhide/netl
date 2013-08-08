
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;

public class DumbSerTest
{		
	public static void main(String[] args) throws IOException
	{
		String txtpath = "/local/fellowship/acquireweb/lserve/iplist/SpecialNeedHP.txt";
		String serpath = "/local/fellowship/acquireweb/lserve/iplist/SpecialNeedHP.ser";
		
		Scanner sc = new Scanner(new File(txtpath));
		TreeSet<Long> myset = Util.treeset();
		for(int i = 0; i < 20000; i++)
		{
			String s = sc.nextLine().trim();
			Long ip = Util.ip2long(s);
			myset.add(ip);
		}
		
		
		Util.pf("Set size is %d, saving\n", myset.size());
		
		FileUtils.serializeEat(myset, serpath);
	}
}
