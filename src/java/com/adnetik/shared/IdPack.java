

package com.adnetik.shared;

import java.util.*;
import java.io.*;

public class IdPack
{	
	private long[] alist;
	private long[] blist;
	
	public IdPack(Set<String> baseids)
	{
		Set<Pair<Long, Long>> bigset = Util.treeset();
		
		for(String oneid : baseids)
		{
			WtpId wid = new WtpId(oneid);
			long[] ab = wid.toLongPair();
			bigset.add(Pair.build(ab[0], ab[1]));
		}
		
		alist = new long[bigset.size()];
		blist = new long[bigset.size()];
		int cc = 0;
		
		for(Pair<Long, Long> onep : bigset)
		{
			alist[cc] = onep._1;
			blist[cc] = onep._2;
			cc++;	
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		// testMany(); 
		// return;
		Scanner scA = new Scanner(new File("input_ids.txt"));
		Set<String> idset = Util.treeset();
		
		while(scA.hasNextLine())
		{
			String oneid = scA.nextLine().trim();
			
			try { 
				WtpId wid = new WtpId(oneid); 
				idset.add(oneid);
			} catch (Exception ex) {
				// Util.pf("Invalid wtpid %s\n", oneid);	
			}
		}
		
		IdPack idpack = new IdPack(idset);
		Util.pf("\nBuild IdPack, contains %d elements", idpack.alist.length);
		
		{
			Scanner testA = new Scanner(new File("file_a.txt"));
			
			while(testA.hasNextLine())
			{
				String oneid = testA.nextLine().trim();
				
				try { WtpId wid = new WtpId(oneid); }
				catch ( Exception ex ) { continue; }				
				
				boolean a = idset.contains(oneid);
				boolean b = idpack.contains(oneid);
				
				Util.massert(a == b);
				
				Util.pf("For id %s, a=%b, b=%b\n", oneid, a, b);
			}
		}
	}		
	
	//public static void transformToFile(Set<String> idset, String filepath
	
	
	public boolean contains(String id)
	{
		WtpId wid = new WtpId(id);	
		return contains(wid);
	}
	
	public boolean contains(WtpId wid)
	{
		long[] ab = wid.toLongPair();
		
		int lookup = Arrays.binarySearch(alist, ab[0]);
		
		if(lookup >= 0)
		{
			for(int i = lookup; i < alist.length && alist[i] == ab[0]; i++)
			{
				if(blist[i] == ab[1])
					{ return true; }
			}
		}
		
		return false;
	}
	
	
	public static void main2(String[] args) throws Exception
	{
		// testMany(); 
		// return;
		Random jr = new Random();
		Scanner sc = new Scanner(new File("test_my_ids.txt"));
		
		while(sc.hasNextLine())
		{
			String oneid = sc.nextLine().trim();
			if(oneid.length() == 0)
				{ continue; }
		
			
			long[] src = new long[] { jr.nextLong(), 0 };
			WtpId test = WtpId.fromLongPair(src);
			long[] dst = test.toLongPair();
			
			Util.pf("SRC=%d, DST=%d\n", src[0], dst[0]);
			Util.massert(src[0] == dst[0]);
			
			WtpId test2 = new WtpId(oneid);
			//test2.zeroUpperData();
			long[] ab = test2.toLongPair();
			WtpId copy = WtpId.fromLongPair(ab);	
			
			Util.pf("Test=%s, copy=%s\n", test2, copy);
			Util.massert(test2.toString().equals(copy.toString()));
		}
	}
	
	public static void testMany()
	{
		for(short s = 0; s < 260; s++)
		{
			
			//printShort(s);	
		}
	
		Random jr = new Random();
		
		for(int i = 0; i < 100000; i++)
		{
			long src = jr.nextLong();
			testBackForth(src);
			
			/*
			byte[] d = WtpId.long2bytes(src);
			long dst = WtpId.bytes2long(d);	
			Util.pf("\nSRc=%d, DST = %d\n", src, dst);
			Util.massert(src == dst);
			*/
		}
	}
	
	public static void printShort(short x)
	{
		StringBuffer sb = new StringBuffer();
		short mask = 1;
		
		for(int i = 15; i >= 0; i--)
		{
			int a = (mask << i) & x;				
			sb.append((a > 0 ? "1" : "0"));
		}
		
		Util.pf("\nShort value %d is:\n\t%s", x, sb.toString());
	}
	
	public static void testBackForth(long src)
	{
		byte[] data = Util.long2bytes(src);
		long dst = Util.bytes2long(data);
		
		/*
		for(int i = 0; i < 64; i++)
		{
			boolean set = getBit(src, i);
			
			if(set)
				dst = setBit(dst, i);
		}
		*/
		
		Util.pf("SRC=%d, DST=%d\n", src, dst);
		Util.massert(src == dst);
	}
	

	
	

		
	
}

