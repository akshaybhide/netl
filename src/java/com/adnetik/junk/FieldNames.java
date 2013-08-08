
package com.adnetik.shared;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;

import com.adnetik.shared.Util.*;

public class FieldNames
{	
	private static ConcurrentHashMap<LogType, ConcurrentHashMap<LogVersion, ConcurrentHashMap<String, Integer>>> _LOOKUP = Util.conchashmap(); 
	
	static {
		(new Version21()).initFields();		
		(new Version20()).initFields();
		(new Version19()).initFields();				
		(new Version18()).initFields();		
		(new Version17()).initFields();
		(new Version16()).initFields();
		(new Version15()).initFields();
		(new Version14()).initFields();
		(new Version13()).initFields();
		(new Version12()).initFields();
		(new UserIndexFormat2()).initFields();
	}
	
	public static boolean haveVersion4Type(LogType ltype, LogVersion lvers)
	{
		return _LOOKUP.containsKey(ltype) && _LOOKUP.get(ltype).containsKey(lvers);
	}
	
	public static boolean hasField(LogType ltype, LogVersion lvers, String fCode)
	{
		return _LOOKUP.get(ltype).get(lvers).containsKey(fCode);
	}
	
	public static int getFieldId(LogType ltype, LogVersion lvers, String fCode)
	{
		Map<String, Integer> fmap = getFieldMap(ltype, lvers);
		
		// Util.massert(fmap.containsKey(fCode)
		
		Integer fid = fmap.get(fCode);
		
		if(fid == null)
		{
			int numFields = fmap.size();
			throw new RuntimeException("Unknown field code: " + fCode + " for ltype/version " + ltype + " " + lvers);
		}
		
		return fid;
	}
	
	public static int getFieldCount(LogType ltype, LogVersion lvers)
	{
		return getFieldMap(ltype, lvers).size();
	}
	
	public static String[] getFieldList(LogType ltype, LogVersion lvers)
	{
		Map<String, Integer> fmap = getFieldMap(ltype, lvers);
		String[] flist = new String[fmap.size()];
		
		for(String fname : fmap.keySet())
			{ flist[fmap.get(fname)] = fname; }

		return flist;
	}

	public static Map<String, Integer> getFieldMap(LogType ltype, LogVersion lvers)
	{
		return Collections.unmodifiableMap(_LOOKUP.get(ltype).get(lvers));
	}
	
	private static void popMapFromList(LogType ltype, LogVersion lvers, List<String> fieldList)
	{
		ConcurrentHashMap<String, Integer> relmap = Util.conchashmap();
		
		for(String s : fieldList)
			{ relmap.put(s, relmap.size()); }
		
		if(!_LOOKUP.containsKey(ltype))
		{ 
			ConcurrentHashMap<LogVersion, ConcurrentHashMap<String, Integer>> m = Util.conchashmap();
			_LOOKUP.put(ltype, m);
		}
		
		Util.massert(!_LOOKUP.get(ltype).containsKey(lvers), "Already called popMap for %s, %s", ltype, lvers);
		
		_LOOKUP.get(ltype).put(lvers, relmap);
	}
	
	/*
	private static class UserIndexFormat
	{
		void initFields()
		{
			List<String> nblist = Util.vector();
			nblist.add("date_time");
			nblist.add("ad_exchange");
			nblist.add("url");
			nblist.add("domain");
			nblist.add("url_keywords");
			nblist.add("google_main_vertical");
			nblist.add("user_ip");
			nblist.add("user_country");
			nblist.add("user_region");
			nblist.add("user_DMA");
			nblist.add("user_city");
			nblist.add("user_postal");
			nblist.add("user_language");
			nblist.add("browser");
			nblist.add("os");
			nblist.add("wtp_user_id");
			nblist.add("time_zone");
			nblist.add("size"); // add this for error-checking sanity
			// nblist.add("segment_info"); segment info doesn't go into BigData logs
			
			popMapFromList(LogType.UIndexMinType, LogVersion.UIndexMinVers, nblist);
		}
	}
	*/
	
	// Adding extra fields to support IAB 
	private static class UserIndexFormat2
	{
		void initFields()
		{
			List<String> nblist = Util.vector();
			nblist.add("date_time");
			nblist.add("ad_exchange");
			nblist.add("url");
			nblist.add("domain");
			nblist.add("url_keywords");
			nblist.add("google_main_vertical");
			nblist.add("user_ip");
			nblist.add("user_country");
			nblist.add("user_region");
			nblist.add("user_DMA");
			nblist.add("user_city");
			nblist.add("user_postal");
			nblist.add("user_language");
			nblist.add("browser");
			nblist.add("os");
			nblist.add("wtp_user_id");
			nblist.add("time_zone");
			nblist.add("size"); 
			
			// These must match against fields used in BidLogxEntry::getExcSegIdList
			nblist.add("google_verticals_slicer"); // use this instead of google_verticals, b/c its in ICC logs also
			nblist.add("contextweb_categories");
			nblist.add("openx_category_tier1");
			nblist.add("openx_category_tier2");
			nblist.add("rubicon_site_channel_id");
			nblist.add("yahoo_pub_channel");
			nblist.add("nexage_content_categories");
			
			// add this for error-checking sanity
			// nblist.add("segment_info"); segment info doesn't go into BigData logs
			
			popMapFromList(LogType.UIndexMinType, LogVersion.UIndexMinVers2, nblist);
		}
	}	
	
	
	// No changes!!! 
	// These changes only affect bid and no_bid
	private static class Version21 extends Version20
	{
		@Override
		LogVersion getVersion()
		{
			return LogVersion.v21;	
		}
	}		
	
	
	private static class Version20 extends Version19
	{
		@Override
		LogVersion getVersion()
		{
			return LogVersion.v20;	
		}
		
		@Override
		List<String> getBidContext()
		{
			List<String> a = new Vector<String>(super.getBidContext());
			a.add("deal_id");
			a.add("deal_price");
			a.add("appnexus_creative_id");
			a.add("facebook_page_type");
			a.add("pubmatic_pub_id");
			a.add("pubmatic_site_id");
			a.add("pubmatic_ad_id");
			return a;
		}		
	}		
	
	private static class Version19 extends Version18
	{
		@Override
		LogVersion getVersion()
		{
			return LogVersion.v19;	
		}
		
		@Override
		List<String> getBidContext()
		{
			List<String> a = new Vector<String>(super.getBidContext());
			a.add("content");
			return a;
		}		
	}		
	
	
	private static class Version18 extends Version17
	{
		@Override
		LogVersion getVersion()
		{
			return LogVersion.v18;	
		}
		
		@Override
		List<String> getBidContext()
		{
			List<String> a = new Vector<String>(super.getBidContext());
			a.add("utw");
			return a;
		}		
		
		@Override
		void initImp()
		{
			List<String> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add("uuid");
			implist.add("winner_price");
			implist.addAll(getBidContext());
			implist.add("segment_info");
			implist.add("dbh_macro");
			popMapFromList(LogType.imp, getVersion(), implist);
		}	
		
		@Override
		void initClick()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("redirect_url");
			xlist.addAll(getBidContext());
			xlist.add("segment_info");	
			xlist.add("dbh_macro");			
			popMapFromList(LogType.click, getVersion(), xlist);
		}	
		
		@Override
		void initConversion()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("conversion_id");
			xlist.addAll(getBidContext());
			xlist.add("is_post_view");
			xlist.add("is_post_click");
			xlist.add("segment_info");	
			xlist.add("dbh_macro");			
			popMapFromList(LogType.conversion, getVersion(), xlist);
		}					
	}			
	
	private static class Version17 extends Version16
	{
		@Override
		LogVersion getVersion()
		{
			return LogVersion.v17;	
		}
		
		@Override
		List<String> getBidContext()
		{
			List<String> a = new Vector<String>(super.getBidContext());
			a.add("ua_device_type");
			a.add("ua_device_maker");
			a.add("ua_device_model");
			a.add("ua_os");
			a.add("ua_os_version");
			a.add("ua_browser");
			a.add("ua_browser_version");
			a.add("is_mobile_app");			
			return a;
		}		
	}			
	
	private static class Version16
	{
		LogVersion getVersion()
		{
			return LogVersion.v16;
		}
		
		void initFields()
		{
			initNoBidAll();
			initNoBid();
			initBidAll();
			initImp();
			initConversion();
			initClick();			
		}
			
		void initNoBidAll()
		{
			List<String> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, getVersion(), nblist);
		}
		
		void initNoBid()
		{
			List<String> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			nblist.add("compression_ratio");
			popMapFromList(LogType.no_bid, getVersion(), nblist);
		}		
		
		
		void initBidPrefilt()
		{
			List<String> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.bid_pre_filtered, getVersion(), nblist);			
		}
		
		void initBidAll()
		{
			List<String> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, getVersion(), balist);
		}
		
		void initImp()
		{
			List<String> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add("uuid");
			implist.add("winner_price");
			implist.addAll(getBidContext());
			implist.add("segment_info");
			popMapFromList(LogType.imp, getVersion(), implist);
		}		
		
		void initClick()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("redirect_url");
			xlist.addAll(getBidContext());
			xlist.add("segment_info");			
			popMapFromList(LogType.click, getVersion(), xlist);
		}				
		
		void initConversion()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("conversion_id");
			xlist.addAll(getBidContext());
			xlist.add("is_post_view");
			xlist.add("is_post_click");
			xlist.add("segment_info");			
			popMapFromList(LogType.conversion, getVersion(), xlist);
		}			
		
		List<String> getStandardHeader()
		{
			List<String> a = new Vector<String>((new Version15()).getStandardHeader());
			return a;
		}
		
		List<String> getBidContext()
		{
			List<String> a = new Vector<String>((new Version15()).getBidContext());
			a.add("adscale_slot_id");
			a.add("liveintent_publisher_id");
			a.add("liveintent_site_id");
			a.add("liveintent_publisher_categories");
			a.add("liveintent_site_categories");
			return a;
		}
	}			
	
	private static class Version15
	{
		void initFields()
		{
			initNoBid();
			initBidAll();
			initImp();
			initConversion();
			initClick();			
		}
		
		void initNoBid()
		{
			List<String> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v15, nblist);
		}
		
		void initBidAll()
		{
			List<String> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v15, balist);
		}
		
		void initImp()
		{
			List<String> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add("uuid");
			implist.add("winner_price");
			implist.addAll(getBidContext());
			implist.add("segment_info");
			popMapFromList(LogType.imp, LogVersion.v15, implist);
		}		
		
		void initClick()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("redirect_url");
			xlist.addAll(getBidContext());
			xlist.add("segment_info");			
			popMapFromList(LogType.click, LogVersion.v15, xlist);
		}				
		
		void initConversion()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("conversion_id");
			xlist.addAll(getBidContext());
			xlist.add("is_post_view");
			xlist.add("is_post_click");
			xlist.add("segment_info");			
			popMapFromList(LogType.conversion, LogVersion.v15, xlist);
		}			
		
		List<String> getStandardHeader()
		{
			List<String> mlist = Util.vector();
			mlist.add("date_time");
			mlist.add("ip");
			mlist.add("country");
			mlist.add("region");
			mlist.add("request_uri");
			mlist.add("referer");
			mlist.add("useragent");
			return mlist;			
		}
		
		List<String> getBidContext()
		{
			List<String> a = Util.vector();
			a.add("ad_exchange");
			a.add("auction_id");
			a.add("impression_id");
			a.add("bid");
			a.add("advertiser_id");
			a.add("campaign_id");
			a.add("line_item_id");
			a.add("line_item_type");
			a.add("creative_id");
			a.add("size");
			a.add("adaptv_creative_type");
			a.add("auction_type");
			a.add("advertiser_pricing_type");
			a.add("url");
			a.add("domain");
			a.add("url_keywords");
			a.add("referrer_url");
			a.add("referrer_domain");
			a.add("referrer_url_keywords");
			a.add("visibility");
			a.add("tag_format");
			a.add("within_iframe");
			a.add("publisher_pricing_type");
			a.add("google_adslot_id");
			a.add("google_verticals");
			a.add("google_main_vertical");
			a.add("google_main_vertical_with_weight");
			a.add("google_verticals_slicer");
			a.add("google_anonymous_id");
			a.add("admeld_website_id");
			a.add("admeld_publisher_id");
			a.add("admeld_tag_id");
			a.add("adnexus_tag_id");
			a.add("adnexus_inventory_class");
			a.add("openx_website_id");
			a.add("openx_placement_id");
			a.add("openx_category_tier1");
			a.add("openx_category_tier2");
			a.add("rubicon_website_id");
			a.add("rubicon_site_channel_id");
			a.add("rubicon_site_name");
			a.add("rubicon_domain_name");
			a.add("rubicon_page_url");
			a.add("improve_digital_website_id");
			a.add("yahoo_buyer_line_item_id");
			a.add("yahoo_exchange_id");
			a.add("yahoo_pub_channel");
			a.add("yahoo_seller_id");
			a.add("yahoo_seller_line_item_id");
			a.add("yahoo_section_id");
			a.add("yahoo_segment_id");
			a.add("yahoo_site_id");
			a.add("adbrite_zone_id");
			a.add("adbrite_zone_url");
			a.add("adbrite_zone_quality");
			a.add("adaptv_is_top");
			a.add("adaptv_placement_name");
			a.add("adaptv_placement_id");
			a.add("adaptv_placement_topics");
			a.add("adaptv_placement_quality");
			a.add("adaptv_placement_metrics");
			a.add("adaptv_video_id");
			a.add("casale_website_channel_id");
			a.add("casale_website_id");
			a.add("cox_content_categories");
			a.add("contextweb_categories");
			a.add("contextweb_tag_id");
			a.add("adjug_publisher_id");
			a.add("adjug_site_id");
			a.add("dbh_publisher_id");
			a.add("dbh_site_id");
			a.add("dbh_placement_id");
			a.add("dbh_ad_tag_type");
			a.add("dbh_default_type");
			a.add("nexage_content_categories");
			a.add("nexage_publisher_id");
			a.add("nexage_site_id");
			a.add("user_ip");
			a.add("user_country");
			a.add("user_region");
			a.add("user_DMA");
			a.add("user_city");
			a.add("user_postal");
			a.add("user_language");
			a.add("language");
			a.add("user_agent");
			a.add("browser");
			a.add("os");
			a.add("time_zone");
			a.add("exchange_user_id");
			a.add("wtp_user_id");
			a.add("view_count");
			a.add("no_flash");
			a.add("age");
			a.add("gender");
			a.add("ethnicity");
			a.add("marital");
			a.add("kids");
			a.add("hhi");
			a.add("is_test");
			a.add("load_i5m");
			a.add("load_i9h");
			a.add("no_sync");
			a.add("mobile_carrier");
			a.add("mobile_loc");
			a.add("mobile_device_id");
			a.add("mobile_device_platform_id");
			a.add("mobile_device_make");
			a.add("mobile_device_model");
			a.add("mobile_device_js");
			a.add("conversion_type");
			a.add("adnexus_reserve_price");
			a.add("adnexus_estimated_clear_price");
			a.add("adnexus_estimated_average_price");
			a.add("adnexus_estimated_price_verified");
			a.add("adbrite_current_time");
			a.add("adbrite_previous_page_view_time");
			a.add("admeta_site_categories");
			a.add("admeta_site_id");
			a.add("spotx_categories");
			a.add("real_domain");
			a.add("real_referrer_domain");
			a.add("real_url");
			a.add("real_referrer_url");
			a.add("reporting_type");
			a.add("currency");
			a.add("transaction_id");
			a.add("retargeting_timelimit");
			return a;
		}
	}		
	
	
	private static class Version14
	{
		void initFields()
		{
			initNoBid();
			initBidAll();
			initImp();
			initConversion();
			initClick();			
		}
		
		void initNoBid()
		{
			List<String> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v14, nblist);
		}
		
		void initBidAll()
		{
			List<String> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v14, balist);
		}
		
		void initImp()
		{
			List<String> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add("uuid");
			implist.add("winner_price");
			implist.addAll(getBidContext());
			implist.add("segment_info");
			popMapFromList(LogType.imp, LogVersion.v14, implist);
		}		
		
		void initClick()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("redirect_url");
			xlist.addAll(getBidContext());
			xlist.add("segment_info");			
			popMapFromList(LogType.click, LogVersion.v14, xlist);
		}				
		
		void initConversion()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("conversion_id");
			xlist.addAll(getBidContext());
			xlist.add("is_post_view");
			xlist.add("is_post_click");
			xlist.add("segment_info");			
			popMapFromList(LogType.conversion, LogVersion.v14, xlist);
		}			
		
		List<String> getStandardHeader()
		{
			List<String> mlist = Util.vector();
			mlist.add("date_time");
			mlist.add("ip");
			mlist.add("country");
			mlist.add("region");
			mlist.add("request_uri");
			mlist.add("referer");
			mlist.add("useragent");
			return mlist;			
		}
		
		List<String> getBidContext()
		{
			List<String> a = Util.vector();
			a.add("ad_exchange");
			a.add("auction_id");
			a.add("impression_id");
			a.add("bid");
			a.add("advertiser_id");
			a.add("campaign_id");
			a.add("line_item_id");
			a.add("line_item_type");
			a.add("creative_id");
			a.add("size");
			a.add("adaptv_creative_type");
			a.add("auction_type");
			a.add("advertiser_pricing_type");
			a.add("url");
			a.add("domain");
			a.add("url_keywords");
			a.add("referrer_url");
			a.add("referrer_domain");
			a.add("referrer_url_keywords");
			a.add("visibility");
			a.add("tag_format");
			a.add("within_iframe");
			a.add("publisher_pricing_type");
			a.add("google_adslot_id");
			a.add("google_verticals");
			a.add("google_main_vertical");
			a.add("google_main_vertical_with_weight");
			a.add("google_verticals_slicer");
			a.add("google_anonymous_id");
			a.add("admeld_website_id");
			a.add("admeld_publisher_id");
			a.add("admeld_tag_id");
			a.add("adnexus_tag_id");
			a.add("adnexus_inventory_class");
			a.add("openx_website_id");
			a.add("openx_placement_id");
			a.add("openx_category_tier1");
			a.add("openx_category_tier2");
			a.add("rubicon_website_id");
			a.add("rubicon_site_channel_id");
			a.add("rubicon_site_name");
			a.add("rubicon_domain_name");
			a.add("rubicon_page_url");
			a.add("improve_digital_website_id");
			a.add("yahoo_buyer_line_item_id");
			a.add("yahoo_exchange_id");
			a.add("yahoo_pub_channel");
			a.add("yahoo_seller_id");
			a.add("yahoo_seller_line_item_id");
			a.add("yahoo_section_id");
			a.add("yahoo_segment_id");
			a.add("yahoo_site_id");
			a.add("adbrite_zone_id");
			a.add("adbrite_zone_url");
			a.add("adbrite_zone_quality");
			a.add("adaptv_is_top");
			a.add("adaptv_placement_name");
			a.add("adaptv_placement_id");
			a.add("adaptv_placement_topics");
			a.add("adaptv_placement_quality");
			a.add("adaptv_placement_metrics");
			a.add("adaptv_video_id");
			a.add("casale_website_channel_id");
			a.add("casale_website_id");
			a.add("cox_content_categories");
			a.add("contextweb_categories");
			a.add("contextweb_tag_id");
			a.add("adjug_publisher_id");
			a.add("adjug_site_id");
			a.add("dbh_publisher_id");
			a.add("dbh_site_id");
			a.add("dbh_placement_id");
			a.add("dbh_ad_tag_type");
			a.add("dbh_default_type");
			a.add("nexage_content_categories");
			a.add("nexage_publisher_id");
			a.add("nexage_site_id");
			a.add("user_ip");
			a.add("user_country");
			a.add("user_region");
			a.add("user_DMA");
			a.add("user_city");
			a.add("user_postal");
			a.add("user_language");
			a.add("language");
			a.add("user_agent");
			a.add("browser");
			a.add("os");
			a.add("time_zone");
			a.add("exchange_user_id");
			a.add("wtp_user_id");
			a.add("view_count");
			a.add("no_flash");
			a.add("age");
			a.add("gender");
			a.add("ethnicity");
			a.add("marital");
			a.add("kids");
			a.add("hhi");
			a.add("is_test");
			a.add("load_i5m");
			a.add("load_i9h");
			a.add("no_sync");
			a.add("mobile_carrier");
			a.add("mobile_loc");
			a.add("mobile_device_id");
			a.add("mobile_device_platform_id");
			a.add("mobile_device_make");
			a.add("mobile_device_model");
			a.add("mobile_device_js");
			a.add("conversion_type");
			a.add("adnexus_reserve_price");
			a.add("adnexus_estimated_clear_price");
			a.add("adnexus_estimated_average_price");
			a.add("adnexus_estimated_price_verified");
			a.add("adbrite_current_time");
			a.add("adbrite_previous_page_view_time");
			a.add("admeta_site_categories");
			a.add("admeta_site_id");
			a.add("spotx_categories");
			a.add("real_domain");
			a.add("real_referrer_domain");
			a.add("real_url");
			a.add("real_referrer_url");
			a.add("reporting_type");
			return a;
		}
	}	
	
	
	private static class Version13
	{
		void initFields()
		{
			initNoBid();
			initBidAll();
			initImp();
			initConversion();
			initClick();			
		}
		
		void initNoBid()
		{
			List<String> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v13, nblist);
		}
		
		void initBidAll()
		{
			List<String> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v13, balist);
		}
		
		void initImp()
		{
			List<String> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add("uuid");
			implist.add("winner_price");
			implist.addAll(getBidContext());
			popMapFromList(LogType.imp, LogVersion.v13, implist);
		}		
		
		void initClick()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("redirect_url");
			xlist.addAll(getBidContext());
			popMapFromList(LogType.click, LogVersion.v13, xlist);
		}				
		
		void initConversion()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("conversion_id");
			xlist.addAll(getBidContext());
			xlist.add("is_post_view");
			xlist.add("is_post_click");
			popMapFromList(LogType.conversion, LogVersion.v13, xlist);
		}			
		
		List<String> getStandardHeader()
		{
			List<String> mlist = Util.vector();
			mlist.add("date_time");
			mlist.add("ip");
			mlist.add("country");
			mlist.add("region");
			mlist.add("request_uri");
			mlist.add("referer");
			mlist.add("useragent");
			return mlist;			
		}
		
		List<String> getBidContext()
		{
			List<String> a = Util.vector();
			a.add("ad_exchange");
			a.add("auction_id");
			a.add("impression_id");
			a.add("bid");
			a.add("advertiser_id");
			a.add("campaign_id");
			a.add("line_item_id");
			a.add("line_item_type");
			a.add("creative_id");
			a.add("size");
			a.add("adaptv_creative_type");
			a.add("auction_type");
			a.add("advertiser_pricing_type");
			a.add("url");
			a.add("domain");
			a.add("real_domain");
			a.add("url_keywords");
			a.add("referrer_url");
			a.add("referrer_domain");
			a.add("real_referrer_domain");
			a.add("referrer_url_keywords");
			a.add("visibility");
			a.add("tag_format");
			a.add("within_iframe");
			a.add("publisher_pricing_type");
			a.add("google_adslot_id");
			a.add("google_verticals");
			a.add("google_main_vertical");
			a.add("google_main_vertical_with_weight");
			a.add("google_verticals_slicer");
			a.add("google_anonymous_id");
			a.add("admeld_website_id");
			a.add("admeld_publisher_id");
			a.add("admeld_tag_id");
			a.add("adnexus_tag_id");
			a.add("adnexus_inventory_class");
			a.add("openx_website_id");
			a.add("openx_placement_id");
			a.add("openx_category_tier1");
			a.add("openx_category_tier2");
			a.add("rubicon_website_id");
			a.add("rubicon_site_channel_id");
			a.add("rubicon_site_name");
			a.add("rubicon_domain_name");
			a.add("rubicon_page_url");
			a.add("improve_digital_website_id");
			a.add("yahoo_buyer_line_item_id");
			a.add("yahoo_exchange_id");
			a.add("yahoo_pub_channel");
			a.add("yahoo_seller_id");
			a.add("yahoo_seller_line_item_id");
			a.add("yahoo_section_id");
			a.add("yahoo_segment_id");
			a.add("yahoo_site_id");
			a.add("adbrite_zone_id");
			a.add("adbrite_zone_url");
			a.add("adbrite_zone_quality");
			a.add("adaptv_is_top");
			a.add("adaptv_placement_name");
			a.add("adaptv_placement_id");
			a.add("adaptv_placement_topics");
			a.add("adaptv_placement_quality");
			a.add("adaptv_placement_metrics");
			a.add("adaptv_video_id");
			a.add("casale_website_channel_id");
			a.add("casale_website_id");
			a.add("cox_content_categories");
			a.add("contextweb_categories");
			a.add("contextweb_tag_id");
			a.add("adjug_publisher_id");
			a.add("adjug_site_id");
			a.add("dbh_publisher_id");
			a.add("dbh_site_id");
			a.add("dbh_placement_id");
			a.add("dbh_ad_tag_type");
			a.add("dbh_default_type");
			a.add("nexage_content_categories");
			a.add("nexage_publisher_id");
			a.add("nexage_site_id");
			a.add("user_ip");
			a.add("user_country");
			a.add("user_region");
			a.add("user_DMA");
			a.add("user_city");
			a.add("user_postal");
			a.add("user_language");
			a.add("language");
			a.add("user_agent");
			a.add("browser");
			a.add("os");
			a.add("time_zone");
			a.add("exchange_user_id");
			a.add("wtp_user_id");
			a.add("view_count");
			a.add("no_flash");
			a.add("age");
			a.add("gender");
			a.add("ethnicity");
			a.add("marital");
			a.add("kids");
			a.add("hhi");
			a.add("is_test");
			a.add("load_i5m");
			a.add("load_i9h");
			a.add("no_sync");
			a.add("mobile_carrier");
			a.add("mobile_loc");
			a.add("mobile_device_id");
			a.add("mobile_device_platform_id");
			a.add("mobile_device_make");
			a.add("mobile_device_model");
			a.add("mobile_device_js");
			a.add("conversion_type");
			a.add("adnexus_reserve_price");
			a.add("adnexus_estimated_clear_price");
			a.add("adnexus_estimated_average_price");
			a.add("adnexus_estimated_price_verified");
			a.add("adbrite_current_time");
			a.add("adbrite_previous_page_view_time");
			return a;
		}
	}
	
	
	private static class Version12
	{	
		
		void initFields()
		{
			initNoBid();
			initBidAll();
			initImp();
			initConversion();
			initClick();			
		}
		
		void initNoBid()
		{
			List<String> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v12, nblist);
		}
		
		void initBidAll()
		{
			List<String> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v12, balist);
		}
		
		void initImp()
		{
			List<String> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add("uuid");
			implist.add("winner_price");
			implist.addAll(getBidContext());
			popMapFromList(LogType.imp, LogVersion.v12, implist);
		}		
		
		void initClick()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("redirect_url");
			xlist.addAll(getBidContext());
			popMapFromList(LogType.click, LogVersion.v12, xlist);
		}				
		
		void initConversion()
		{
			List<String> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add("uuid");
			xlist.add("conversion_id");
			xlist.addAll(getBidContext());
			xlist.add("is_post_view");
			xlist.add("is_post_click");
			popMapFromList(LogType.conversion, LogVersion.v12, xlist);
		}			
		
		public static List<String> getStandardHeader()
		{
			List<String> mlist = new Vector<String>();
			
			mlist.add("date_time");
			mlist.add("ip");
			mlist.add("country");
			mlist.add("region");
			mlist.add("request_uri");
			mlist.add("referer");
			mlist.add("useragent");
			
			return mlist;
		}
		
		public static List<String> getBidContext()
		{
			List<String> mlist = new Vector<String>();
			
			mlist.add("ad_exchange");
			mlist.add("auction_id");
			mlist.add("impression_id");
			mlist.add("bid");
			mlist.add("advertiser_id");
			mlist.add("campaign_id");
			mlist.add("line_item_id");
			mlist.add("line_item_type");
			mlist.add("creative_id");
			mlist.add("size");
			mlist.add("adaptv_creative_type");
			mlist.add("url");
			mlist.add("domain");
			mlist.add("url_keywords");
			mlist.add("referrer_url");
			mlist.add("referrer_domain");
			mlist.add("referrer_url_keywords");
			mlist.add("visibility");
			mlist.add("tag_format");
			mlist.add("within_iframe");
			mlist.add("google_adslot_id");
			mlist.add("google_verticals");
			mlist.add("google_main_vertical");
			mlist.add("google_main_vertical_with_weight");
			mlist.add("google_verticals_slicer");
			mlist.add("google_anonymous_id");
			mlist.add("admeld_website_id");
			mlist.add("admeld_publisher_id");
			mlist.add("admeld_tag_id");
			mlist.add("adnexus_tag_id");
			mlist.add("adnexus_inventory_class");
			mlist.add("openx_website_id");
			mlist.add("openx_placement_id");
			mlist.add("openx_category_tier1");
			mlist.add("openx_category_tier2");
			mlist.add("rubicon_website_id");
			mlist.add("rubicon_site_channel_id");
			mlist.add("rubicon_site_name");
			mlist.add("rubicon_domain_name");
			mlist.add("rubicon_page_url");
			mlist.add("improve_digital_website_id");
			mlist.add("yahoo_buyer_line_item_id");
			mlist.add("yahoo_exchange_id");
			mlist.add("yahoo_pub_channel");
			mlist.add("yahoo_seller_id");
			mlist.add("yahoo_seller_line_item_id");
			mlist.add("yahoo_section_id");
			mlist.add("yahoo_segment_id");
			mlist.add("yahoo_site_id");
			mlist.add("adbrite_zone_id");
			mlist.add("adbrite_zone_url");
			mlist.add("adbrite_zone_quality");
			mlist.add("adaptv_is_top");
			mlist.add("adaptv_placement_name");
			mlist.add("adaptv_placement_id");
			mlist.add("adaptv_placement_topics");
			mlist.add("adaptv_placement_quality");
			mlist.add("adaptv_placement_metrics");
			mlist.add("adaptv_video_id");
			mlist.add("casale_website_channel_id");
			mlist.add("casale_website_id");
			mlist.add("cox_content_categories");
			mlist.add("contextweb_categories");
			mlist.add("contextweb_tag_id");
			mlist.add("adjug_publisher_id");
			mlist.add("adjug_site_id");
			mlist.add("dbh_publisher_id");
			mlist.add("dbh_site_id");
			mlist.add("dbh_section_id");
			mlist.add("nexage_content_categories");
			mlist.add("nexage_publisher_id");
			mlist.add("nexage_site_id");
			mlist.add("user_ip");
			mlist.add("user_country");
			mlist.add("user_region");
			mlist.add("user_DMA");
			mlist.add("user_city");
			mlist.add("user_postal");
			mlist.add("user_language");
			mlist.add("language");
			mlist.add("user_agent");
			mlist.add("browser");
			mlist.add("os");
			mlist.add("time_zone");
			mlist.add("exchange_user_id");
			mlist.add("wtp_user_id");
			mlist.add("view_count");
			mlist.add("no_flash");
			mlist.add("age");
			mlist.add("gender");
			mlist.add("ethnicity");
			mlist.add("marital");
			mlist.add("kids");
			mlist.add("hhi");
			mlist.add("is_test");
			mlist.add("load_i5m");
			mlist.add("load_i9h");
			mlist.add("no_sync");
			mlist.add("mobile_carrier");
			mlist.add("mobile_loc");
			mlist.add("mobile_device_id");
			mlist.add("mobile_device_platform_id");
			mlist.add("mobile_device_make");
			mlist.add("mobile_device_model");
			mlist.add("mobile_device_js");
			mlist.add("conversion_type");
			mlist.add("adnexus_reserve_price");
			mlist.add("adnexus_estimated_clear_price");
			mlist.add("adnexus_estimated_average_price");
			mlist.add("adnexus_estimated_price_verified");
			mlist.add("adbrite_current_time");
			mlist.add("adbrite_previous_page_view_time");
			
			return mlist;
		}		
		
	}
	
	private static void showMapDiff(Map<String, Integer> a, Map<String, Integer> b)
	{
		for(String akey : a.keySet())
		{
			if(!b.containsKey(akey))
			{
				Util.pf("\n\tSrc field name %s not found in target map", akey);	
			}
		}		
	}
	
	public static void main(String[] args)
	{
		Map<String, String> optargs = Util.getClArgMap(args);

		Util.pf("Hello, world");
	}
}
