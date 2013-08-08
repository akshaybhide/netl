
package com.adnetik.shared;

import java.util.*;
import java.io.*;

import com.adnetik.shared.Util.*;

public class BidLogEntry extends LogEntry
{	  
	// TODO: this is a copy of BmUtil.CurrCode, should centralize
	private enum SubCurrCode { USD, GBP, EUR };
	
	public static final LogField[] BASIC_SCRUB = new LogField[] { LogField.request_uri, LogField.user_agent }; 
	
	public enum FormatErrCode { AD_SIZE_ERR, IP_FORMAT_ERR, WTP_ID_ERR, NO_SYNC_ERR, 
		CURRCODE_ERR,
		WINNER_PRICE_ERR,
		BID_ERR,
		EXCHANGE_FORMAT_ERR,
		RANDOM_ERR, // fake "random" error, use to check error code that doesn't throw errors often
		WINNER_PRICE, BID, GENERIC, CAMPLINE_ERR, TRUNC_ERR
		
	};
	
	//public static final String BID_LOG_CHARSET = "US-ASCII";	
	public static final String BID_LOG_CHARSET = "ISO-8859-1";
	
			
	// Just store this as an array. We will know the number of fields at 
	// creation time. Don't want to use a map, because that will increase memory consumption.
	private String[] fields;
	
	LogType ltype;
	LogVersion lvers;
	
	private Pair<ExcName, List<Integer>> _excSegIdList = null;
	
	
	// Creates a dummy entry with blank fields
	private BidLogEntry(LogType targType, LogVersion targVers)
	{
		ltype = targType;
		lvers = targVers;
		
		fields = new String[FieldLookup.getFieldCount(ltype, lvers)];
		
		for(int i = 0; i < fields.length; i++)
			{ fields[i] = ""; }
	}
	
	
	public BidLogEntry(LogType targType, LogVersion targVers, String logline) throws BidLogFormatException
	{
		ltype = targType;
		lvers = targVers;
		
		fields = logline.split("\t");
		basicCheck();		
	}
	
	public LogType getLogType()
	{
		return 	ltype;
	}
	
	public LogVersion getLogVersion()
	{
		return lvers;	
	}
	
	public BidLogEntry transformToVersion(LogType newtype, LogVersion newvers)
	{
		BidLogEntry newentry = new BidLogEntry(newtype, newvers);
		
		// Get all the fields that exist in the current version, and try to set them in the 
		// new version.
		for(LogField fname : FieldLookup.getFieldMap(newtype, newvers).keySet())
		{
			if(checkFieldPresent(fname))
			{
				//Util.pf("\n\tCopying field %s", fname);
				newentry.setField(fname, getField(fname));
			} else if (!FieldLookup.hasField(ltype, lvers, fname)) {
				
				Util.pf("\n\t Field %s not present in new format", fname);
			}
		}
		
		// newentry.basicCheck();
		return newentry;		
	}
	
	@Override
	public String getDerivedField(LogField fname)
	{
		// This one only works for the BidLogs, because we don't have timezone in the pixel logs
		if(fname == LogField.user_hour)
		{
			// eg 2010-02-15 14:55:41 	
			String[] date_time = getField(LogField.date_time).trim().split(" ");
			int loghour = Integer.valueOf(date_time[1].split(":")[0]);			
			
			// Transform hour in log file to GMT 
			int log2gmt = (TimeUtil.isSummer(date_time[0]) ? 4 : 5);
			
			// Just bail out here
			Integer gmt2usr = SharedResource.getDefaultTimezoneMinutes(getField(LogField.user_country), getField(LogField.user_region));
			if(gmt2usr == null)
				{ return ""; }
			
			gmt2usr /= 60;
			
			// +24, then mod 24 is the idiom that does what I want			
			int reshour = ((loghour + log2gmt + gmt2usr) + 24) % 24;
			
			return ("") + reshour;
		}
		
		return super.getDerivedField(fname);
	}
	
	// This is basically a version upgrape
	public BidLogEntry transformToVersion(LogVersion newvers) throws BidLogFormatException
	{
		return transformToVersion(ltype, newvers);
	}
	
	public String getLogLine()
	{
		return Util.join(fields, "\t");
	}
	
	
	// This is more convenient than the constructor if you don't
	// want to do any fruity Exception logging stuff
	public static BidLogEntry getOrNull(LogType targType, LogVersion targVers, String logline) 
	{
		try { 
			BidLogEntry ble = new BidLogEntry(targType, targVers, logline);
			return ble;
		} catch (BidLogFormatException blex) {
			return null;	
		}
	}		

	public static String getSuffCode(LogType ltype, LogVersion vers)
	{
		return Util.sprintf("%s%s%s", ltype, Util.DUMB_SEP, vers);
	}
	
	public void setField(LogField key, String value)
	{
		int fid = getFieldId(key);
		fields[fid] = value;
	}
	
	public String[] getFields()
	{ 
		return fields; 
	}
	
	public void basicScrub()
	{
		for(LogField s : BASIC_SCRUB)
		{
			setField(s, "");
		}
	}
	
	public boolean hasField(LogField fname)
	{
		return FieldLookup.hasField(ltype, lvers, fname);
	}
	
	public boolean checkFieldPresent(LogField fname)
	{
		if(!FieldLookup.hasField(ltype, lvers, fname))
			return false;
		
		return fields.length > FieldLookup.getFieldId(ltype, lvers, fname);
	}
	
	public String getField(LogField fname)
	{
		if(fname.isDerived())
			{ return getDerivedField(fname); }
		
		// This throws an exception if the field name is not present
		int fid = FieldLookup.getFieldId(ltype, lvers, fname); 
		
		if(fid >= fields.length)
			{ return ""; }
		
		// This check just does not work often enough
		// Util.massert(fid < fields.length,
		//	"Attempt to access field %s at position %d, but have only %d fields in entry, perhaps record is corrupt?",
		//	fname, fid, fields.length);
		
		return fields[fid];
	}
	
	public String getFieldWithEx(LogField fname) throws BidLogFormatException
	{
		// This throws an exception if the field name is not present
		int fid = FieldLookup.getFieldId(ltype, lvers, fname); 
		
		if(fid >= fields.length)
		{
			String errmessg = Util.sprintf("Attempt to access field %s at position %d, but have only %d fields in entry, perhaps record is corrupt?",
								fname, fid, fields.length);
			
			throw new BidLogFormatException(errmessg, FormatErrCode.TRUNC_ERR);	
		}
		
		return fields[fid];
	}	
	
	
	public int getFieldCount()
	{
		return fields.length;	
	}
	
	public String getFieldDirect(int i)
	{
		return fields[i];	
	}
	
	@Deprecated
	public int getFieldId(String fname)
	{
		return getFieldId(LogField.valueOf(fname));	
	}
	
	public int getFieldId(LogField fname)
	{
		// This throws an exception if the field name is not present
		return getFieldId(ltype, lvers, fname); 	
	}
	
	public static int getFieldId(LogType lt, LogVersion lv, LogField fname)
	{
		return FieldLookup.getFieldId(lt, lv, fname);
	}
	
	public void basicCheck() throws BidLogFormatException
	{
		adSizeCheck();
		//noSyncCheck();
	}
	
	public void currencyCodeCheck() throws BidLogFormatException
	{
		// other log types (click/conversion) have currency field in theory, 
		// but it is not really meaningful
		if(ltype == LogType.click || ltype == LogType.conversion)
			{ return; } 
		
		if(!hasField(LogField.currency))
			{ return; }
		
		String currstr = getField(LogField.currency);
		
		try { 
			SubCurrCode cc = SubCurrCode.valueOf(currstr);
		} catch (IllegalArgumentException ex) {
			throw new BidLogFormatException(
				Util.sprintf("Illegal currency code '%s'", currstr), 
				FormatErrCode.CURRCODE_ERR);
		}
	}
	
	public void campaignLineAdIdCheck() throws BidLogFormatException
	{
		// Added UTW in here
		for(LogField fname : new LogField[] { LogField.campaign_id , LogField.line_item_id, LogField.advertiser_id, LogField.utw })
		{
			if(ltype == LogType.no_bid_all || ltype == LogType.bid_pre_filtered)
				{ return; }
			
			try  { int id = getIntField(fname); }
			catch (Exception ex) 
			{ throw new BidLogFormatException("Error reading field " + fname, FormatErrCode.CAMPLINE_ERR); }
		}
	}
	
	// Check that the cost field is a real number for imp data
	public void costInfoCheck() throws BidLogFormatException
	{
		if(ltype != LogType.imp)
			{ return; }
		
		try  { 
			double cost = getDblField(LogField.winner_price);
		} catch (Exception ex) {
			throw new BidLogFormatException("Error reading winner_price field", FormatErrCode.WINNER_PRICE_ERR); 
		}
	}
		
	// Check that the bid field is populated correctly on Impression and Bid data
	public void bidInfoCheck() throws BidLogFormatException
	{
		if(!(ltype == LogType.imp || ltype == LogType.bid_all))
			{ return; }
		
		try  { 
			double bid = getDblField(LogField.bid);
		} catch (Exception ex) {
			throw new BidLogFormatException("Error reading bid field", FormatErrCode.BID_ERR); 
		}
	}	
	
	
	
	public ExcName getExchangeName() throws BidLogFormatException
	{
		String excstr = getField(LogField.ad_exchange);
		
		try { return Util.excLookup(excstr); }
		catch (Exception ex) {
			throw new BidLogFormatException("Invalid exchange code " + excstr, FormatErrCode.EXCHANGE_FORMAT_ERR); 
		}
	}
	
	public void strictCheck() throws BidLogFormatException
	{
		basicCheck();

		ipCheck();		
		wtpIdCheck();
		userIpCheck();
		
		if(ltype == LogType.conversion)
		{
			try {
				int ipv = getIntField(LogField.is_post_view);
				int ipc = getIntField(LogField.is_post_click);
				
				Util.massert(ipc == 0 || ipc == 1);
				Util.massert(ipv == 0 || ipv == 1);
			} catch (Exception ex) {
				
				throw new BidLogFormatException("Invalid post-view or post-click field");
			}
		}
	}
	
	// Maximum possible strictness
	public void superStrictCheck() throws BidLogFormatException
	{
		strictCheck();
		
		winnerPriceCheck();
		bidCheck();
	}
	
	void winnerPriceCheck() throws BidLogFormatException
	{
		if(!FieldLookup.hasField(ltype, lvers, LogField.winner_price))
			{ return; }
				
		try {
			Double wprice = getDblField(LogField.winner_price);
			
		} catch (Exception ex ) {
			throw new BidLogFormatException("Bad Winner Price", FormatErrCode.WINNER_PRICE);
		}
	}	
	
	void bidCheck() throws BidLogFormatException
	{ 
		if(!FieldLookup.hasField(ltype, lvers, LogField.bid))
			{ return; }
				
		try {
			Double bid = getDblField(LogField.bid);
			
		} catch (Exception ex ) {
			throw new BidLogFormatException("Bad Bid", FormatErrCode.BID);
		}
	}		
	
	public void randomErrCheck(double p, Random jrand) throws BidLogFormatException
	{
		if(jrand.nextDouble() < p)
		{
			throw new BidLogFormatException("Randomly generated error", FormatErrCode.RANDOM_ERR);	
		}
		
	}
	
	void adSizeCheck() throws BidLogFormatException
	{
		String adsize = null;
		
		try {
			adsize = getField(LogField.size).trim();
			
			if(adsize.length() == 0)
				{ return; }			
			
			Util.massert(adsize.indexOf("x") > -1);
			String[] ab = adsize.split("x");
			
			int a = Integer.valueOf(ab[0]);
			int b = Integer.valueOf(ab[1]);
			
		} catch (Exception ex ) {
			throw new BidLogFormatException("Invalid adsize: " + adsize, FormatErrCode.AD_SIZE_ERR);
		}
	}
	
	void ipCheck() throws BidLogFormatException
	{
		/*
		String astr = null;
		
		try {
			astr = getField("size").trim();
			
			// Is this even okay?
			if(astr.length() == 0)
				{ return; }			
			
			
			Util.massert(astr.indexOf("x") > -1);
			String[] ab = astr.split("x");
			
			int a = Integer.valueOf(ab[0]);
			int b = Integer.valueOf(ab[1]);
			
		} catch (Exception ex ) {
			throw new BidLogFormatException("Invalid adsize: " + astr, FormatErrCode.AD_SIZE_ERR);
		}
		*/
	}	
	
	public void noSyncCheck() throws BidLogFormatException
	{
		String astr = null;
		
		try {
			astr = getField(LogField.no_sync).trim();
			Util.massert("0".equals(astr) || "1".equals(astr));
			
		} catch (Exception ex ) {
			throw new BidLogFormatException("Invalid NO_SYNC: " + astr, FormatErrCode.NO_SYNC_ERR);
		}
	}		

	public void uuidCheck() throws BidLogFormatException
	{
		wtpIdCheckSub(LogField.uuid);
	}	
	
	public void wtpIdCheck() throws BidLogFormatException
	{
		wtpIdCheckSub(LogField.wtp_user_id);
	}
	
	private void wtpIdCheckSub(LogField fname) throws BidLogFormatException
	{
		String astr = null;
		
		astr = getField(fname).trim();
		
		if(astr.length() == 0)
			{ return; }			
			

		if(!Util.checkWtp(astr))
		{
			throw new BidLogFormatException("Invalid WTP: " + astr, FormatErrCode.WTP_ID_ERR);
		}
	}		
	
	public void userIpCheck() throws BidLogFormatException
	{
		String ipstr = null;
		
		ExcName excname = getExchangeName();
		
		if(excname == ExcName.facebook || excname == ExcName.rtb || excname == ExcName.adnexus)
			{ return; }
		
		// if(ltype == LogType.no_bid_all 
		
		try {
			ipstr = getField(LogField.user_ip).trim();
			
			if(ipstr.length() == 0)
				{ return; }	
		
			checkIpStr(ipstr);
			
		} catch (Exception ex ) {
			throw new BidLogFormatException("Invalid IP address: " + ipstr, FormatErrCode.IP_FORMAT_ERR);
		}		
	}
	
	public static String checkIpStr(String ipstr)
	{
		boolean hascolon = ipstr.indexOf(":") > -1;
		
		String octetstr = hascolon ? ipstr.split(":")[0] : ipstr;			
		
		String[] f = octetstr.split("\\.");
		
		Util.massert(f.length == 4);
		
		for(String onef : f)
		{
			int a = Integer.valueOf(onef);				
		}
		
		if(hascolon)
		{
			// eg. 112.23.109.202:3030
			String portstr = ipstr.split(":")[1];
			int portno =  Integer.valueOf(portstr);
			//Util.pf("\nPort number is %d", portno);
		}		
		
		return octetstr;
	}
	
	public Set<Integer> getIabSegSet() throws BidLogFormatException
	{
		Set<Integer> iabset = Util.treeset();
		Pair<ExcName, List<Integer>> excinfo = getExcSegIdList();
	
		if(excinfo != null)
		{
			for(Integer excid : excinfo._2)
			{
				Set<Integer> map2set = IABLookup.getSing().excId2IabId(excinfo._1, excid);
				
				if(map2set != null)
				{
					iabset.addAll(map2set);	
				}
			}
		}
		
		return iabset;	
	}
	
	// Cache this thing, to avoid calling it 10000 times in AIDX
	public Pair<ExcName, List<Integer>> getExcSegIdList() throws BidLogFormatException
	{
		if(_excSegIdList == null)
		{
			_excSegIdList = getExcSegIdListSub();
		}
		
		return _excSegIdList;
	}
	
	
	private Pair<ExcName, List<Integer>> getExcSegIdListSub() throws BidLogFormatException
	{
		ExcName exc;
		List<Integer> reslist = Util.vector();
		String flist;
		
		try {
			exc = getExchangeName();
			reslist = Util.vector();
			
			switch (exc) {
				
			case rtb:
				
				String gvinfo = new String(getField(LogField.google_verticals_slicer));
							
				// Classic bug, have to escape the stupid pipe thing
				for(String onetok :  gvinfo.split("\\|"))
				{
					if(onetok.trim().length() == 0)
						{ continue; }
					
					String[] id_wt = onetok.split("_");
					reslist.add(Integer.valueOf(id_wt[0]));
				}				

				break; 
				
			case contextweb:
				
				flist = getField(LogField.contextweb_categories).trim();
				
				if(flist.length() > 0)
				{
					for(String onetok : flist.split(","))
						{ reslist.add(Integer.valueOf(onetok)); }
				}
				break; 
				
			case openx:
				
				// Openx has two category fields
				
				reslist.add(getIntField(LogField.openx_category_tier1)); 					
				reslist.add(getIntField(LogField.openx_category_tier2)); 
				break; 
				
			case rubicon:
				
				reslist.add(getIntField(LogField.rubicon_site_channel_id));
				break; 					
				
			case yahoo:
				
				reslist.add(getIntField(LogField.yahoo_pub_channel));
				break; 					
				
			case nexage:
								
				for(String onetok : getField(LogField.nexage_content_categories).split(","))
				{
					if(!onetok.startsWith("IAB"))
						{ continue; }
					
					Integer id = Integer.valueOf(onetok.substring(3));
					reslist.add(id);
				}
				break; 

			default:
				break;
							
			}		
		} catch (Exception blex) { 
			
			return null;
		}	
		
		return Pair.build(exc, reslist);
		
	}
	
	public static class BidLogFormatException extends Exception
	{
		public FormatErrCode e;
		
		public BidLogFormatException(String m)
		{
			this(m, FormatErrCode.GENERIC);	
		}
		
		public BidLogFormatException(String m, FormatErrCode x)
		{
			super(m);
			e = x;	
		}
	}
}
