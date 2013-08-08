package com.adnetik.bm_etl;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*; 
import com.adnetik.shared.BidLogEntry.*; 

import com.adnetik.bm_etl.BmUtil.*; 

public abstract class BleStructure 
{
	protected BidLogEntry logentry; 
			
	public static BleStructure buildStructure(LogType reltype, LogVersion relvers, String logline)
	throws BidLogFormatException
	{
		BleStructure bles;
		
		if(reltype == LogType.conversion)
			{ bles = new Conversion(); }
		else if(reltype == LogType.click)
			{ bles = new Click(); }
		else if(reltype == LogType.imp)
			{ bles = new Impression(); }
		else if(reltype == LogType.bid_all)
			{ bles = new Bid(); }
		else
			{ throw new RuntimeException("LogType not supported: " + reltype); }
	
		bles.readFields(logline, relvers);
		
		// This will throw a BidLogFormatException if the logentry has improperly formatted
		// campaign_id, line_item_id, or advertiser_id
		bles.logentry.campaignLineAdIdCheck();
		
		return bles;
	}
	
	public void readFields(String logline, LogVersion relvers) throws BidLogFormatException
	{
		logentry = new BidLogEntry(getLogType(), relvers, logline); 
		logentry.basicCheck();
	}
	
	public abstract LogType getLogType();
	
	public BidLogEntry getLogEntry()
	{
		return logentry;	
	}
	
	public abstract Metrics returnMetrics();	
	
	public static class Conversion extends BleStructure {
				
		public LogType getLogType() { return LogType.conversion; } 	
		
		public Metrics returnMetrics()
		{
			Metrics result = new Metrics();
			
			result.setField(IntFact.conversions, 1);			
			result.setField(IntFact.conv_post_click, (isPostClick() ? 1 : 0));
			result.setField(IntFact.conv_post_view,  (isPostView() ? 1 : 0));
			
			return result;		
		}
		
		public boolean isPostView() {
			
			String ipv = logentry.getField(LogField.is_post_view);
			return Integer.valueOf(ipv) == 1;
		}
		
		public boolean isPostClick() {
			String ipc = logentry.getField(LogField.is_post_click);
			return Integer.valueOf(ipc) == 1;
		}
	}	
	
	public static class Click extends BleStructure {
			
		public LogType getLogType() { return LogType.click; }
		
		public Metrics returnMetrics()
		{
			Metrics result = new Metrics();
			result.setField(IntFact.clicks, 1);			
			return result;
		}
	}	
	
	public static class Impression extends BleStructure {
		
		public LogType getLogType() { return LogType.imp; }
		
		public Metrics returnMetrics()
		{
			Metrics result = new Metrics();
			
			result.setField(IntFact.impressions, 1);
						
			// This needs to be standardized somewhere else.
			Double cost = logentry.getDblField(LogField.winner_price);
			result.setField(DblFact.cost, (cost == null ? 0D : cost));

			// The divisor here comes from our bidder, so the unit is always the same.			
			Double bidamount = logentry.getDblField(LogField.bid)/1000;
			result.setField(DblFact.bid_amount, (bidamount == null ? 0D : bidamount));

			// 
			double dp = 0D;
			try { dp = logentry.getDblField(LogField.deal_price); }
			catch (Exception bfex) { }

			result.setField(DblFact.deal_price, dp);
			
			return result;
		}
	}	
	
	public static class Bid extends BleStructure {
		
		public LogType getLogType() { return LogType.bid_all; }
		
		public Metrics returnMetrics()
		{
			Metrics result = new Metrics();
			result.setField(IntFact.bids, 1);
						
			return result;
		}
	}	
	
}
