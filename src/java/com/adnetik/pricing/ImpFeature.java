
package com.adnetik.pricing;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;

public abstract class ImpFeature
{
	public abstract boolean eval(BidLogEntry ble);
	
	public abstract String getCode();
	
	public int evali(BidLogEntry ble)
	{ return eval(ble) ? +1 : -1; }
	
	public static class NullFeat extends ImpFeature
	{
		public boolean eval(BidLogEntry ble)
		{
			return true;
		}
		
		public String getCode() { return "SocialMedia"; }
	}	
	
	public static class SocialMedia extends ImpFeature
	{
		Set<String> smset = Util.treeset();
		
		public SocialMedia()
		{	
			smset.add("facebook.com");
			smset.add("youtube.com");
		}
		
		public boolean eval(BidLogEntry ble)
		{
			String dom = ble.getField("domain");
			return smset.contains(dom);			
		}
		
		public String getCode() { return "SocialMedia"; }
	}
	
	public static class BrowserFeat extends ImpFeature
	{
		String btarg;		
		
		public BrowserFeat(String bt)
		{	
			btarg = bt;
		}
		
		public boolean eval(BidLogEntry ble)
		{
			return btarg.equals(ble.getField("browser"));
		}
		
		public String getCode() { return "Browser=" + btarg; }
	}	
	
	public static class HourOfDay extends ImpFeature
	{
		int targhour;
		
		public HourOfDay(int th)
		{	
			targhour = th;
		}
		
		public boolean eval(BidLogEntry ble)
		{
			int hour = ble.getIntField("hour");
			return (hour == targhour);			
		}
		
		public String getCode() { return Util.sprintf("Hour of Day=%d", targhour); }
	}
		
	
}
