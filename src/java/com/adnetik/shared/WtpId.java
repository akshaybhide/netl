
package com.adnetik.shared;

import java.util.*;
import java.io.*;

import java.math.BigInteger;

public class WtpId implements Comparable<WtpId>
{	
	public static final int VALID_WTP_LENGTH = 36;
	
	byte[] data = new byte[16];

	public static final Set<Integer> DASH_POS = Util.treeset();
	private static byte[] lookup = new byte[256];
	private static String[]	i2str = new String[256];
	
	static {
		DASH_POS.add(23);	
		DASH_POS.add(18);
		DASH_POS.add(13);	
		DASH_POS.add(8);
		
		for(int i = 0; i < 256; i++)
		{
			lookup[i] = (byte) i;
			String s = Integer.toHexString(i);
			i2str[i] = (s.length() == 2 ? s : "0" + s);
		}
	}

	public int compareTo(WtpId other)
	{
		for(int i = 15; i >= 0; i--)
		{
			if(data[i] != other.data[i])
				{ return b2i(data[i]) < b2i(other.data[i]) ? -1 : 1; }
		}		
		
		return 0;
		
		//return toString().compareTo(other.toString());		
	}
	
	@Override
	public int hashCode()
	{
		return toString().hashCode();	
	}

	public static WtpId randomId(Random r)
	{
		WtpId wid = new WtpId();
		r.nextBytes(wid.data);
		return wid;
	}
	
	void zeroUpperData()
	{
		for(int i = 8; i < 16; i++)
			{ data[i] = 0; }
		
	}
	
	public long[] toLongPair()
	{
		byte[] adata = new byte[8];
		byte[] bdata = new byte[8];
		
		System.arraycopy(data, 0, adata, 0, 8);
		System.arraycopy(data, 8, bdata, 0, 8);
		
		long a = Util.bytes2long(adata);
		long b = Util.bytes2long(bdata);
		
		return new long[] { a, b };
	}
	

	public static WtpId fromLongPair(long[] ab)
	{
		WtpId m = new WtpId();
		
		byte[] adata = Util.long2bytes(ab[0]);
		byte[] bdata = Util.long2bytes(ab[1]);
		
		System.arraycopy(adata, 0, m.data, 0, 8);
		System.arraycopy(bdata, 0, m.data, 8, 8);

		return m;		
	}
	

	
	
	public static void main(String[] args) throws IOException
	{		
		/*
		List<String> idlist = FileUtils.readFileLinesE("idlist.txt");
		
		for(String id : idlist)
		{
			WtpId wtpid = new WtpId(id);
			Util.pf("ID is %s\n", wtpid.toString());
			
			long[] longpair = wtpid.toLongPair();
			Util.pf("ID is %s, left is %d, rght is %d\n", 
				wtpid.toString(), longpair[0], longpair[1]);
		}
		*/
		
		String maxlong = Long.MIN_VALUE+ "";
		
		
		// String mystr = "-9223372036854775,808";
		BigInteger mybig = new BigInteger(maxlong);
		// mybig = mybig.subtract(BigInteger.ONE);
		
		Util.pf("Long value is %d\n", mybig.longValue());
		
		{
			long test = mybig.longValue();	
			BigInteger x = new BigInteger(test+"");
			
			Util.massert(x.equals(mybig));
		}
	}
	
	public boolean equals(WtpId other)
	{
		return (compareTo(other) == 0);	
	}
	
	public static WtpId getOrNull(String strform)
	{
		try { return new WtpId(strform); }
		catch (Exception ex) { return null; }
	}
	
	private WtpId()
	{
		
		
	}
	
	public WtpId(String strform)
	{
		int dcount = 0;
		boolean half = false;
		
		if(strform.length() != 36)
		{
			throw new RuntimeException("Invalid wtp: " + strform);	
		}
		
		//Util.pf("\nID is %s", strform);
		
		for(int i = 35; i >= 0; i -= 2)
		{
			if(DASH_POS.contains(i))
				{ i--; }
			
			//Util.pf("\npos is %d, Substring is %s", i, strform.substring(i-1, i+1));
			
			int x = Integer.parseInt(strform.substring(i-1, i+1), 16);
						
			data[dcount++] = i2b(x);
		}
	}
	
	private static byte i2b(int x)
	{ return (byte) x; }
	
	private static int b2i(byte b)
	{
		int x = (int) b;
		return (b < 0 ? b+256 : b);
	}
	
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		
		for(int i = 15; i >= 0; i--)
		{
			sb.append(i2str[b2i(data[i])]);
			
			if(DASH_POS.contains(sb.length()))
				{ sb.append("-"); }
		}
		
		return sb.toString();
	}
	
}

