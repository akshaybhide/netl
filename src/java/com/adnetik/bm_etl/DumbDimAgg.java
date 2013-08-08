package com.adnetik.bm_etl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.WritableComparable;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

public class DumbDimAgg
{	
	// TODO: this should really be Map<DimCode, Integer> 
	private Map<DimCode, String> _data = Util.treemap();
	
	private CurrCode _currency;
	
	public DumbDimAgg(BidLogEntry logentry)
	{
		// Straight transfers
		{
			setField(DimCode.advertiser, logentry.getField(LogField.advertiser_id));
			setField(DimCode.campaign, logentry.getField(LogField.campaign_id));
			setField(DimCode.lineitem, logentry.getField(LogField.line_item_id));
			
			// Okay, in new version, domain is just a straight copy
			String mdomain = logentry.getField(LogField.domain).trim();
			mdomain = mdomain.length() == 0 ? "Unknown" : mdomain;
			setField(DimCode.domain, mdomain);
			
			// Metrocodes are basically just integers, so we do a straight transfer, except if it is unknown.
			String dmastr = logentry.getField(LogField.user_DMA).trim();
			Integer dmaint = (dmastr.length() > 0 ? Integer.valueOf(dmastr) : CatalogUtil.getSing().getUnknownCode(DimCode.metrocode));
			setField(DimCode.metrocode, dmaint);
			
			try {
				int utwval = logentry.getIntField(LogField.utw);
				utwval = utwval < 0 ? 0 : utwval;
				utwval = utwval > 2000 ? 2000 : utwval;
				setField(DimCode.utw, ""+utwval);
			} catch (Exception ex) {
				// TODO: something smarter here
				Util.pf("Error reading UTW field, setting to 0\n");
			}
			
			// Publisher ID
			{
				int pubid = -1;
				
				// Lots of things that could go wrong here
				try {
					ExcName relexc = logentry.getExchangeName();
					if(relexc == ExcName.rtb)
						{ pubid = logentry.getIntField(LogField.dbh_publisher_id); }	
					
					// This number is the max value for Mysql mediumint(9)
					Util.massert(pubid < 8388607);
					
				} catch (Exception ex) { }
				
				setField(DimCode.publisher, pubid+"");
			}
			 
			// Facebook page type
			{
				String fbpagetype = logentry.getField(LogField.facebook_page_type).trim();
				int fbptype = -1;
				try { 
					fbptype = (fbpagetype.length() == 0 ? -1 : Integer.valueOf(fbpagetype));
				} catch (Exception ex) { }
				
				setField(DimCode.fbpagetype, fbptype+"");
			}			
		}
		
		// lookup in catalog table
		transferField(DimCode.country, 		logentry.getField(LogField.user_country));
		transferField(DimCode.size, 		logentry.getField(LogField.size));
		transferField(DimCode.visibility, 	logentry.getField(LogField.visibility));
		transferField(DimCode.language, 	logentry.getField(LogField.user_language));
		transferField(DimCode.gender, 		logentry.getField(LogField.gender));
		transferField(DimCode.browser, 		logentry.getField(LogField.browser));
		transferField(DimCode.exchange, 	logentry.getField(LogField.ad_exchange));
		transferField(DimCode.content,		logentry.getField(LogField.content));
		transferField(DimCode.currcode,		logentry.getField(LogField.currency));	
		
		setCurrency(logentry);
		
		// Util.pf("UTW is %s, CONTENT is %s\n", getField(DimCode.utw), getField(DimCode.content));
		
		// Special trick for looking up combined country/region key, need previously-specified country code as integer
		transferField(DimCode.region, getCountryRegionKey(logentry, this));
		
		// Okay, this is not done here, but in another step, based on pathinfo
		// transferField(DimCode.currcode, logentry.getField("currency"), OTHERS);

		// Special handling of date info
		{
			Calendar logcal = TimeUtil.longDayCode2Cal(logentry.getField(LogField.date_time));
			// BmUtil.timeZoneConvert(logcal, logentry.getField("user_country"), logentry.getField("user_region"));

			String[] d_c_conv = TimeUtil.cal2LongDayCode(logcal).split(" ");
			
			// Integer form of date
			setField(DimCode.date, TimeUtil.dayCode2Int(d_c_conv[0]));
			
			// Hour and quarter
			QuarterCode qcode = QuarterCode.prevQuarterFromTime(d_c_conv[1]);
			setField(DimCode.hour, ""+qcode.getHour());
			setField(DimCode.quarter, ""+qcode.getLongForm());	
			
			//Util.pf("Original time is %s, converted time is %s, country=%s, region=%s\n", 
			//	logentry.getField("date_time"), TimeUtil.cal2LongDayCode(logcal),
			//	logentry.getField("user_country"), logentry.getField("user_region"));
			
			
			//Util.pf("Date=%s, hour=%s, quarter=%s\n", 
			//	getField(DimCode.date), getField(DimCode.hour), getField(DimCode.quarter));
				
		}
		
		updateCreativeInfo(logentry);
	}
	
	// This will return null if the currency has not been set correctly (e.g. data is absent in the log file)
	public CurrCode getCurrency()
	{
		return _currency;
	}
	
	private void setCurrency(BidLogEntry logentry)
	{
		try { _currency = CurrCode.valueOf(logentry.getField(LogField.currency).toUpperCase()); }
		catch (Exception ex) { }
	}
	
	private static String getCountryRegionKey(BidLogEntry logentry, DumbDimAgg dagg)
	{
		String combkey = Util.sprintf("%s%s%s", dagg.getField(DimCode.country), Util.DUMB_SEP, logentry.getField(LogField.user_region).toLowerCase());
		return combkey;
	}
	
	// Subclasses can override this to change the way the creative_id thing is handled.
	protected void updateCreativeInfo(BidLogEntry logentry)
	{
		// This field is actually misnamed in the log. 
		// It is actually either an assignment ID (usually) or an appnexus ID.
		// Rename creative_id in logentry
		int asstappnid = logentry.getIntField(LogField.creative_id);
		boolean isappnexus = logentry.getField(LogField.ad_exchange).toLowerCase().equals(ExcName.adnexus.toString());
		int ctvid = CatalogUtil.getSing().lookupCreativeId(asstappnid, isappnexus);
		
		setField(DimCode.creative, ""+ctvid);
		setField(DimCode.assignment, ""+asstappnid);
	}
	

	
	public String getField(DimCode dc) { return _data.get(dc); }
	
	public void setField(DimCode dc, String s) { _data.put(dc, s); }
	public void setField(DimCode dc, Integer a) { _data.put(dc, a.toString()); }
	
	private void transferField(DimCode acat, String trykey)
	{
		String mkey = trykey.toLowerCase().trim();
		
		if(acat == DimCode.currcode && mkey.length() == 0)
		{ 
			Util.pf("Found log entry with empty currency value, using USD\n");	
			mkey = CurrCode.USD.toString().toLowerCase();	
		}
		
		if(mkey.length() == 0)
		{
			Integer ucode = CatalogUtil.getSing().getUnknownCode(acat);
			
			if(ucode == null)
			{ 
				throw new RuntimeException("Field " + acat + " is unknown, but no unknown code found "); 
			}
			
			setField(acat, ucode);
			return;
		}
		
		Map<String, Integer> lookupmap = CatalogUtil.getSing().logCodeMap.get(acat);
		Integer resval = lookupmap.containsKey(mkey) ? lookupmap.get(mkey) : CatalogUtil.getSing().getOthersCode(acat);
		setField(acat, resval);
	}	
	
	public String computeKey(SortedSet<DimCode> dimset, boolean expandDate) 
	{
		Util.massert(!dimset.isEmpty(), "Attempt to compute key with empty dimset");
		
		int numproc = 0;
		StringBuffer sb = new StringBuffer();
		
		for(DimCode onedim : dimset)
		{ 
			String val = getField(onedim);	
			sb.append(onedim);
			sb.append("=");
			
			if(onedim == DimCode.date && !expandDate && val.length() > 8)
				{ val = val.substring(0, 8); }

			sb.append(val);
			
			numproc++;
			if(numproc < dimset.size())
				{ sb.append("&"); }
		}		
		
		return sb.toString();
	}
	
	/*
	public void updateCurrencyFromPath(PathInfo pinf)
	{
		// In most cases, currency will just be USD
		CurrCode currency = CatalogUtil.getSing().getCurrencyFromPathInfo(pinf);		
		
		// This is going to break if the currency code cannot be found -  that's the correct behavior.
		transferField(DimCode.currcode, currency.toString());
	}
	*/	
}
