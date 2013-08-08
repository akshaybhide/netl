
package com.adnetik.shared;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.io.*;

import com.adnetik.shared.Util.*;

public class FieldLookup
{	
	private static ConcurrentHashMap<LogType, ConcurrentHashMap<LogVersion, ConcurrentHashMap<LogField, Integer>>> _LOOKUP = Util.conchashmap(); 
	
	static {
		(new Version22()).initFields();				
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
	
	public static boolean hasField(LogType ltype, LogVersion lvers, LogField fCode)
	{
		return _LOOKUP.get(ltype).get(lvers).containsKey(fCode);
	}
	
	public static int getFieldId(LogType ltype, LogVersion lvers, LogField fCode)
	{
		Map<LogField, Integer> fmap = getFieldMap(ltype, lvers);
		
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
	
	public static LogField[] getFieldList(LogType ltype, LogVersion lvers)
	{
		Map<LogField, Integer> fmap = getFieldMap(ltype, lvers);
		LogField[] flist = new LogField[fmap.size()];
		
		for(LogField fname : fmap.keySet())
			{ flist[fmap.get(fname)] = fname; }

		return flist;
	}

	public static Map<LogField, Integer> getFieldMap(LogType ltype, LogVersion lvers)
	{
		return Collections.unmodifiableMap(_LOOKUP.get(ltype).get(lvers));
	}
	
	private static void popMapFromList(LogType ltype, LogVersion lvers, List<LogField> fieldList)
	{
		ConcurrentHashMap<LogField, Integer> relmap = Util.conchashmap();
		
		for(LogField s : fieldList)
			{ relmap.put(s, relmap.size()); }
		
		if(!_LOOKUP.containsKey(ltype))
		{ 
			ConcurrentHashMap<LogVersion, ConcurrentHashMap<LogField, Integer>> m = Util.conchashmap();
			_LOOKUP.put(ltype, m);
		}
		
		Util.massert(!_LOOKUP.get(ltype).containsKey(lvers), "Already called popMap for %s, %s", 
			ltype, lvers);
		
		_LOOKUP.get(ltype).put(lvers, relmap);
	}
	
	// Adding extra fields to support IAB 
	private static class UserIndexFormat2
	{
		void initFields()
		{
			List<LogField> nblist = Util.vector();
			
			nblist.add(LogField.date_time);
			nblist.add(LogField.ad_exchange);
			nblist.add(LogField.url);
			nblist.add(LogField.domain);
			nblist.add(LogField.url_keywords);
			nblist.add(LogField.google_main_vertical);
			nblist.add(LogField.user_ip);
			nblist.add(LogField.user_country);
			nblist.add(LogField.user_region);
			nblist.add(LogField.user_DMA);
			nblist.add(LogField.user_city);
			nblist.add(LogField.user_postal);
			nblist.add(LogField.user_language);
			nblist.add(LogField.browser);
			nblist.add(LogField.os);
			nblist.add(LogField.wtp_user_id);
			nblist.add(LogField.time_zone);
			nblist.add(LogField.size); 
			
			// These must match against fields used in BidLogxEntry::getExcSegIdList
			nblist.add(LogField.google_verticals_slicer); // use this instead of google_verticals, b/c its in ICC logs also
			nblist.add(LogField.contextweb_categories);
			nblist.add(LogField.openx_category_tier1);
			nblist.add(LogField.openx_category_tier2);
			nblist.add(LogField.rubicon_site_channel_id);
			nblist.add(LogField.yahoo_pub_channel);
			nblist.add(LogField.nexage_content_categories);
			
			// add this for error-checking sanity
			// nblist.add(LogField.segment_info); segment info doesn't go into BigData logs
			
			popMapFromList(LogType.UIndexMinType, LogVersion.UIndexMinVers2, nblist);
		}
	}	
	
	// Significant changes here
	private static class Version22 extends Version21
	{
		@Override
		LogVersion getVersion()
		{
			return LogVersion.v22;	
		}

		@Override
		void initNoBidAll()
		{
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			nblist.add(LogField.loss_reason_code);			
			nblist.add(LogField.random_prefiltered_user);
			smartMapPopList(LogType.no_bid_all, nblist);
		}
		
		void initBidAll()
		{
			List<LogField> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			balist.add(LogField.loss_reason_code);						
			balist.add(LogField.random_prefiltered_user);
			smartMapPopList(LogType.bid_all, balist);
		}
		
		void initImp()
		{
			List<LogField> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add(LogField.uuid);
			implist.add(LogField.winner_price);
			implist.addAll(getBidContext());
			implist.add(LogField.segment_info);
			implist.add(LogField.dbh_macro);
			implist.add(LogField.publisher_payout);
			smartMapPopList(LogType.imp, implist);
		}		
		
		// Click has no extra stuff for v22
		
		void initConversion()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			xlist.add(LogField.segment_info);
			xlist.add(LogField.dbh_macro);
			xlist.add(LogField.conversion_interval);
			smartMapPopList(LogType.conversion, xlist);
		}					
		
		
		@Override
		List<LogField> getBidContext()
		{
			List<LogField> a = new Vector<LogField>(super.getBidContext());
			a.add(LogField.user_agent_hash);
			a.add(LogField.targeting_criteria);
			a.add(LogField.iab_category);			
			a.add(LogField.cookie_born);
			a.add(LogField.real_creative_id);
			a.add(LogField.impression_timestamp);
			a.add(LogField.cpa_amount);
			a.add(LogField.top_10_segments_flag);
			a.add(LogField.bid_floor_cpm);
			return a;
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
		List<LogField> getBidContext()
		{
			List<LogField> a = new Vector<LogField>(super.getBidContext());
			a.add(LogField.deal_id);
			a.add(LogField.deal_price);
			a.add(LogField.appnexus_creative_id);
			a.add(LogField.facebook_page_type);
			a.add(LogField.pubmatic_pub_id);
			a.add(LogField.pubmatic_site_id);
			a.add(LogField.pubmatic_ad_id);
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
		List<LogField> getBidContext()
		{
			List<LogField> a = new Vector<LogField>(super.getBidContext());
			a.add(LogField.content);
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
		List<LogField> getBidContext()
		{
			List<LogField> a = new Vector<LogField>(super.getBidContext());
			a.add(LogField.utw);
			return a;
		}		
		
		@Override
		void initImp()
		{
			List<LogField> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add(LogField.uuid);
			implist.add(LogField.winner_price);
			implist.addAll(getBidContext());
			implist.add(LogField.segment_info);
			implist.add(LogField.dbh_macro);
			popMapFromList(LogType.imp, getVersion(), implist);
		}	
		
		@Override
		void initClick()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.redirect_url);
			xlist.addAll(getBidContext());
			xlist.add(LogField.segment_info);	
			xlist.add(LogField.dbh_macro);			
			popMapFromList(LogType.click, getVersion(), xlist);
		}	
		
		@Override
		void initConversion()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			xlist.add(LogField.segment_info);	
			xlist.add(LogField.dbh_macro);			
			popMapFromList(LogType.conversion, getVersion(), xlist);
		}		
		
		@Override
		void initActivity()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			xlist.add(LogField.segment_info);	
			xlist.add(LogField.dbh_macro);			
			popMapFromList(LogType.activity, getVersion(), xlist);
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
		List<LogField> getBidContext()
		{
			List<LogField> a = new Vector<LogField>(super.getBidContext());
			a.add(LogField.ua_device_type);
			a.add(LogField.ua_device_maker);
			a.add(LogField.ua_device_model);
			a.add(LogField.ua_os);
			a.add(LogField.ua_os_version);
			a.add(LogField.ua_browser);
			a.add(LogField.ua_browser_version);
			a.add(LogField.is_mobile_app);			
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
			initActivity();
		}
			
		void initNoBidAll()
		{
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, getVersion(), nblist);
		}
		
		void initNoBid()
		{
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			nblist.add(LogField.compression_ratio);
			popMapFromList(LogType.no_bid, getVersion(), nblist);
		}		
		
		
		void initBidPrefilt()
		{
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.bid_pre_filtered, getVersion(), nblist);			
		}
		
		void initBidAll()
		{
			List<LogField> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, getVersion(), balist);
		}
		
		void initImp()
		{
			List<LogField> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add(LogField.uuid);
			implist.add(LogField.winner_price);
			implist.addAll(getBidContext());
			implist.add(LogField.segment_info);
			popMapFromList(LogType.imp, getVersion(), implist);
		}		
		
		void initClick()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.redirect_url);
			xlist.addAll(getBidContext());
			xlist.add(LogField.segment_info);			
			popMapFromList(LogType.click, getVersion(), xlist);
		}				
		
		void initConversion()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			xlist.add(LogField.segment_info);			
			popMapFromList(LogType.conversion, getVersion(), xlist);
		}	

		void initActivity()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			xlist.add(LogField.segment_info);			
			popMapFromList(LogType.activity, getVersion(), xlist);
		}	
		
		
		List<LogField> getStandardHeader()
		{
			List<LogField> a = new Vector<LogField>((new Version15()).getStandardHeader());
			return a;
		}
		
		List<LogField> getBidContext()
		{
			List<LogField> a = new Vector<LogField>((new Version15()).getBidContext());
			a.add(LogField.adscale_slot_id);
			a.add(LogField.liveintent_publisher_id);
			a.add(LogField.liveintent_site_id);
			a.add(LogField.liveintent_publisher_categories);
			a.add(LogField.liveintent_site_categories);
			return a;
		}
		
		protected void smartMapPopList(LogType ltype, List<LogField> lflist)
		{
			popMapFromList(ltype, getVersion(), lflist);	
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
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v15, nblist);
		}
		
		void initBidAll()
		{
			List<LogField> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v15, balist);
		}
		
		void initImp()
		{
			List<LogField> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add(LogField.uuid);
			implist.add(LogField.winner_price);
			implist.addAll(getBidContext());
			implist.add(LogField.segment_info);
			popMapFromList(LogType.imp, LogVersion.v15, implist);
		}		
		
		void initClick()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.redirect_url);
			xlist.addAll(getBidContext());
			xlist.add(LogField.segment_info);			
			popMapFromList(LogType.click, LogVersion.v15, xlist);
		}				
		
		void initConversion()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			xlist.add(LogField.segment_info);			
			popMapFromList(LogType.conversion, LogVersion.v15, xlist);
		}			
		
		List<LogField> getStandardHeader()
		{
			List<LogField> mlist = Util.vector();
			mlist.add(LogField.date_time);
			mlist.add(LogField.ip);
			mlist.add(LogField.country);
			mlist.add(LogField.region);
			mlist.add(LogField.request_uri);
			mlist.add(LogField.referer);
			mlist.add(LogField.useragent);
			return mlist;			
		}
		
		List<LogField> getBidContext()
		{
			List<LogField> a = Util.vector();
			a.add(LogField.ad_exchange);
			a.add(LogField.auction_id);
			a.add(LogField.impression_id);
			a.add(LogField.bid);
			a.add(LogField.advertiser_id);
			a.add(LogField.campaign_id);
			a.add(LogField.line_item_id);
			a.add(LogField.line_item_type);
			a.add(LogField.creative_id);
			a.add(LogField.size);
			a.add(LogField.adaptv_creative_type);
			a.add(LogField.auction_type);
			a.add(LogField.advertiser_pricing_type);
			a.add(LogField.url);
			a.add(LogField.domain);
			a.add(LogField.url_keywords);
			a.add(LogField.referrer_url);
			a.add(LogField.referrer_domain);
			a.add(LogField.referrer_url_keywords);
			a.add(LogField.visibility);
			a.add(LogField.tag_format);
			a.add(LogField.within_iframe);
			a.add(LogField.publisher_pricing_type);
			a.add(LogField.google_adslot_id);
			a.add(LogField.google_verticals);
			a.add(LogField.google_main_vertical);
			a.add(LogField.google_main_vertical_with_weight);
			a.add(LogField.google_verticals_slicer);
			a.add(LogField.google_anonymous_id);
			a.add(LogField.admeld_website_id);
			a.add(LogField.admeld_publisher_id);
			a.add(LogField.admeld_tag_id);
			a.add(LogField.adnexus_tag_id);
			a.add(LogField.adnexus_inventory_class);
			a.add(LogField.openx_website_id);
			a.add(LogField.openx_placement_id);
			a.add(LogField.openx_category_tier1);
			a.add(LogField.openx_category_tier2);
			a.add(LogField.rubicon_website_id);
			a.add(LogField.rubicon_site_channel_id);
			a.add(LogField.rubicon_site_name);
			a.add(LogField.rubicon_domain_name);
			a.add(LogField.rubicon_page_url);
			a.add(LogField.improve_digital_website_id);
			a.add(LogField.yahoo_buyer_line_item_id);
			a.add(LogField.yahoo_exchange_id);
			a.add(LogField.yahoo_pub_channel);
			a.add(LogField.yahoo_seller_id);
			a.add(LogField.yahoo_seller_line_item_id);
			a.add(LogField.yahoo_section_id);
			a.add(LogField.yahoo_segment_id);
			a.add(LogField.yahoo_site_id);
			a.add(LogField.adbrite_zone_id);
			a.add(LogField.adbrite_zone_url);
			a.add(LogField.adbrite_zone_quality);
			a.add(LogField.adaptv_is_top);
			a.add(LogField.adaptv_placement_name);
			a.add(LogField.adaptv_placement_id);
			a.add(LogField.adaptv_placement_topics);
			a.add(LogField.adaptv_placement_quality);
			a.add(LogField.adaptv_placement_metrics);
			a.add(LogField.adaptv_video_id);
			a.add(LogField.casale_website_channel_id);
			a.add(LogField.casale_website_id);
			a.add(LogField.cox_content_categories);
			a.add(LogField.contextweb_categories);
			a.add(LogField.contextweb_tag_id);
			a.add(LogField.adjug_publisher_id);
			a.add(LogField.adjug_site_id);
			a.add(LogField.dbh_publisher_id);
			a.add(LogField.dbh_site_id);
			a.add(LogField.dbh_placement_id);
			a.add(LogField.dbh_ad_tag_type);
			a.add(LogField.dbh_default_type);
			a.add(LogField.nexage_content_categories);
			a.add(LogField.nexage_publisher_id);
			a.add(LogField.nexage_site_id);
			a.add(LogField.user_ip);
			a.add(LogField.user_country);
			a.add(LogField.user_region);
			a.add(LogField.user_DMA);
			a.add(LogField.user_city);
			a.add(LogField.user_postal);
			a.add(LogField.user_language);
			a.add(LogField.language);
			a.add(LogField.user_agent);
			a.add(LogField.browser);
			a.add(LogField.os);
			a.add(LogField.time_zone);
			a.add(LogField.exchange_user_id);
			a.add(LogField.wtp_user_id);
			a.add(LogField.view_count);
			a.add(LogField.no_flash);
			a.add(LogField.age);
			a.add(LogField.gender);
			a.add(LogField.ethnicity);
			a.add(LogField.marital);
			a.add(LogField.kids);
			a.add(LogField.hhi);
			a.add(LogField.is_test);
			a.add(LogField.load_i5m);
			a.add(LogField.load_i9h);
			a.add(LogField.no_sync);
			a.add(LogField.mobile_carrier);
			a.add(LogField.mobile_loc);
			a.add(LogField.mobile_device_id);
			a.add(LogField.mobile_device_platform_id);
			a.add(LogField.mobile_device_make);
			a.add(LogField.mobile_device_model);
			a.add(LogField.mobile_device_js);
			a.add(LogField.conversion_type);
			a.add(LogField.adnexus_reserve_price);
			a.add(LogField.adnexus_estimated_clear_price);
			a.add(LogField.adnexus_estimated_average_price);
			a.add(LogField.adnexus_estimated_price_verified);
			a.add(LogField.adbrite_current_time);
			a.add(LogField.adbrite_previous_page_view_time);
			a.add(LogField.admeta_site_categories);
			a.add(LogField.admeta_site_id);
			a.add(LogField.spotx_categories);
			a.add(LogField.real_domain);
			a.add(LogField.real_referrer_domain);
			a.add(LogField.real_url);
			a.add(LogField.real_referrer_url);
			a.add(LogField.reporting_type);
			a.add(LogField.currency);
			a.add(LogField.transaction_id);
			a.add(LogField.retargeting_timelimit);
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
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v14, nblist);
		}
		
		void initBidAll()
		{
			List<LogField> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v14, balist);
		}
		
		void initImp()
		{
			List<LogField> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add(LogField.uuid);
			implist.add(LogField.winner_price);
			implist.addAll(getBidContext());
			implist.add(LogField.segment_info);
			popMapFromList(LogType.imp, LogVersion.v14, implist);
		}		
		
		void initClick()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.redirect_url);
			xlist.addAll(getBidContext());
			xlist.add(LogField.segment_info);			
			popMapFromList(LogType.click, LogVersion.v14, xlist);
		}				
		
		void initConversion()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			xlist.add(LogField.segment_info);			
			popMapFromList(LogType.conversion, LogVersion.v14, xlist);
		}			
		
		List<LogField> getStandardHeader()
		{
			List<LogField> mlist = Util.vector();
			mlist.add(LogField.date_time);
			mlist.add(LogField.ip);
			mlist.add(LogField.country);
			mlist.add(LogField.region);
			mlist.add(LogField.request_uri);
			mlist.add(LogField.referer);
			mlist.add(LogField.useragent);
			return mlist;			
		}
		
		List<LogField> getBidContext()
		{
			List<LogField> a = Util.vector();
			a.add(LogField.ad_exchange);
			a.add(LogField.auction_id);
			a.add(LogField.impression_id);
			a.add(LogField.bid);
			a.add(LogField.advertiser_id);
			a.add(LogField.campaign_id);
			a.add(LogField.line_item_id);
			a.add(LogField.line_item_type);
			a.add(LogField.creative_id);
			a.add(LogField.size);
			a.add(LogField.adaptv_creative_type);
			a.add(LogField.auction_type);
			a.add(LogField.advertiser_pricing_type);
			a.add(LogField.url);
			a.add(LogField.domain);
			a.add(LogField.url_keywords);
			a.add(LogField.referrer_url);
			a.add(LogField.referrer_domain);
			a.add(LogField.referrer_url_keywords);
			a.add(LogField.visibility);
			a.add(LogField.tag_format);
			a.add(LogField.within_iframe);
			a.add(LogField.publisher_pricing_type);
			a.add(LogField.google_adslot_id);
			a.add(LogField.google_verticals);
			a.add(LogField.google_main_vertical);
			a.add(LogField.google_main_vertical_with_weight);
			a.add(LogField.google_verticals_slicer);
			a.add(LogField.google_anonymous_id);
			a.add(LogField.admeld_website_id);
			a.add(LogField.admeld_publisher_id);
			a.add(LogField.admeld_tag_id);
			a.add(LogField.adnexus_tag_id);
			a.add(LogField.adnexus_inventory_class);
			a.add(LogField.openx_website_id);
			a.add(LogField.openx_placement_id);
			a.add(LogField.openx_category_tier1);
			a.add(LogField.openx_category_tier2);
			a.add(LogField.rubicon_website_id);
			a.add(LogField.rubicon_site_channel_id);
			a.add(LogField.rubicon_site_name);
			a.add(LogField.rubicon_domain_name);
			a.add(LogField.rubicon_page_url);
			a.add(LogField.improve_digital_website_id);
			a.add(LogField.yahoo_buyer_line_item_id);
			a.add(LogField.yahoo_exchange_id);
			a.add(LogField.yahoo_pub_channel);
			a.add(LogField.yahoo_seller_id);
			a.add(LogField.yahoo_seller_line_item_id);
			a.add(LogField.yahoo_section_id);
			a.add(LogField.yahoo_segment_id);
			a.add(LogField.yahoo_site_id);
			a.add(LogField.adbrite_zone_id);
			a.add(LogField.adbrite_zone_url);
			a.add(LogField.adbrite_zone_quality);
			a.add(LogField.adaptv_is_top);
			a.add(LogField.adaptv_placement_name);
			a.add(LogField.adaptv_placement_id);
			a.add(LogField.adaptv_placement_topics);
			a.add(LogField.adaptv_placement_quality);
			a.add(LogField.adaptv_placement_metrics);
			a.add(LogField.adaptv_video_id);
			a.add(LogField.casale_website_channel_id);
			a.add(LogField.casale_website_id);
			a.add(LogField.cox_content_categories);
			a.add(LogField.contextweb_categories);
			a.add(LogField.contextweb_tag_id);
			a.add(LogField.adjug_publisher_id);
			a.add(LogField.adjug_site_id);
			a.add(LogField.dbh_publisher_id);
			a.add(LogField.dbh_site_id);
			a.add(LogField.dbh_placement_id);
			a.add(LogField.dbh_ad_tag_type);
			a.add(LogField.dbh_default_type);
			a.add(LogField.nexage_content_categories);
			a.add(LogField.nexage_publisher_id);
			a.add(LogField.nexage_site_id);
			a.add(LogField.user_ip);
			a.add(LogField.user_country);
			a.add(LogField.user_region);
			a.add(LogField.user_DMA);
			a.add(LogField.user_city);
			a.add(LogField.user_postal);
			a.add(LogField.user_language);
			a.add(LogField.language);
			a.add(LogField.user_agent);
			a.add(LogField.browser);
			a.add(LogField.os);
			a.add(LogField.time_zone);
			a.add(LogField.exchange_user_id);
			a.add(LogField.wtp_user_id);
			a.add(LogField.view_count);
			a.add(LogField.no_flash);
			a.add(LogField.age);
			a.add(LogField.gender);
			a.add(LogField.ethnicity);
			a.add(LogField.marital);
			a.add(LogField.kids);
			a.add(LogField.hhi);
			a.add(LogField.is_test);
			a.add(LogField.load_i5m);
			a.add(LogField.load_i9h);
			a.add(LogField.no_sync);
			a.add(LogField.mobile_carrier);
			a.add(LogField.mobile_loc);
			a.add(LogField.mobile_device_id);
			a.add(LogField.mobile_device_platform_id);
			a.add(LogField.mobile_device_make);
			a.add(LogField.mobile_device_model);
			a.add(LogField.mobile_device_js);
			a.add(LogField.conversion_type);
			a.add(LogField.adnexus_reserve_price);
			a.add(LogField.adnexus_estimated_clear_price);
			a.add(LogField.adnexus_estimated_average_price);
			a.add(LogField.adnexus_estimated_price_verified);
			a.add(LogField.adbrite_current_time);
			a.add(LogField.adbrite_previous_page_view_time);
			a.add(LogField.admeta_site_categories);
			a.add(LogField.admeta_site_id);
			a.add(LogField.spotx_categories);
			a.add(LogField.real_domain);
			a.add(LogField.real_referrer_domain);
			a.add(LogField.real_url);
			a.add(LogField.real_referrer_url);
			a.add(LogField.reporting_type);
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
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v13, nblist);
		}
		
		void initBidAll()
		{
			List<LogField> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v13, balist);
		}
		
		void initImp()
		{
			List<LogField> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add(LogField.uuid);
			implist.add(LogField.winner_price);
			implist.addAll(getBidContext());
			popMapFromList(LogType.imp, LogVersion.v13, implist);
		}		
		
		void initClick()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.redirect_url);
			xlist.addAll(getBidContext());
			popMapFromList(LogType.click, LogVersion.v13, xlist);
		}				
		
		void initConversion()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			popMapFromList(LogType.conversion, LogVersion.v13, xlist);
		}			
		
		List<LogField> getStandardHeader()
		{
			List<LogField> mlist = Util.vector();
			mlist.add(LogField.date_time);
			mlist.add(LogField.ip);
			mlist.add(LogField.country);
			mlist.add(LogField.region);
			mlist.add(LogField.request_uri);
			mlist.add(LogField.referer);
			mlist.add(LogField.useragent);
			return mlist;			
		}
		
		List<LogField> getBidContext()
		{
			List<LogField> a = Util.vector();
			a.add(LogField.ad_exchange);
			a.add(LogField.auction_id);
			a.add(LogField.impression_id);
			a.add(LogField.bid);
			a.add(LogField.advertiser_id);
			a.add(LogField.campaign_id);
			a.add(LogField.line_item_id);
			a.add(LogField.line_item_type);
			a.add(LogField.creative_id);
			a.add(LogField.size);
			a.add(LogField.adaptv_creative_type);
			a.add(LogField.auction_type);
			a.add(LogField.advertiser_pricing_type);
			a.add(LogField.url);
			a.add(LogField.domain);
			a.add(LogField.real_domain);
			a.add(LogField.url_keywords);
			a.add(LogField.referrer_url);
			a.add(LogField.referrer_domain);
			a.add(LogField.real_referrer_domain);
			a.add(LogField.referrer_url_keywords);
			a.add(LogField.visibility);
			a.add(LogField.tag_format);
			a.add(LogField.within_iframe);
			a.add(LogField.publisher_pricing_type);
			a.add(LogField.google_adslot_id);
			a.add(LogField.google_verticals);
			a.add(LogField.google_main_vertical);
			a.add(LogField.google_main_vertical_with_weight);
			a.add(LogField.google_verticals_slicer);
			a.add(LogField.google_anonymous_id);
			a.add(LogField.admeld_website_id);
			a.add(LogField.admeld_publisher_id);
			a.add(LogField.admeld_tag_id);
			a.add(LogField.adnexus_tag_id);
			a.add(LogField.adnexus_inventory_class);
			a.add(LogField.openx_website_id);
			a.add(LogField.openx_placement_id);
			a.add(LogField.openx_category_tier1);
			a.add(LogField.openx_category_tier2);
			a.add(LogField.rubicon_website_id);
			a.add(LogField.rubicon_site_channel_id);
			a.add(LogField.rubicon_site_name);
			a.add(LogField.rubicon_domain_name);
			a.add(LogField.rubicon_page_url);
			a.add(LogField.improve_digital_website_id);
			a.add(LogField.yahoo_buyer_line_item_id);
			a.add(LogField.yahoo_exchange_id);
			a.add(LogField.yahoo_pub_channel);
			a.add(LogField.yahoo_seller_id);
			a.add(LogField.yahoo_seller_line_item_id);
			a.add(LogField.yahoo_section_id);
			a.add(LogField.yahoo_segment_id);
			a.add(LogField.yahoo_site_id);
			a.add(LogField.adbrite_zone_id);
			a.add(LogField.adbrite_zone_url);
			a.add(LogField.adbrite_zone_quality);
			a.add(LogField.adaptv_is_top);
			a.add(LogField.adaptv_placement_name);
			a.add(LogField.adaptv_placement_id);
			a.add(LogField.adaptv_placement_topics);
			a.add(LogField.adaptv_placement_quality);
			a.add(LogField.adaptv_placement_metrics);
			a.add(LogField.adaptv_video_id);
			a.add(LogField.casale_website_channel_id);
			a.add(LogField.casale_website_id);
			a.add(LogField.cox_content_categories);
			a.add(LogField.contextweb_categories);
			a.add(LogField.contextweb_tag_id);
			a.add(LogField.adjug_publisher_id);
			a.add(LogField.adjug_site_id);
			a.add(LogField.dbh_publisher_id);
			a.add(LogField.dbh_site_id);
			a.add(LogField.dbh_placement_id);
			a.add(LogField.dbh_ad_tag_type);
			a.add(LogField.dbh_default_type);
			a.add(LogField.nexage_content_categories);
			a.add(LogField.nexage_publisher_id);
			a.add(LogField.nexage_site_id);
			a.add(LogField.user_ip);
			a.add(LogField.user_country);
			a.add(LogField.user_region);
			a.add(LogField.user_DMA);
			a.add(LogField.user_city);
			a.add(LogField.user_postal);
			a.add(LogField.user_language);
			a.add(LogField.language);
			a.add(LogField.user_agent);
			a.add(LogField.browser);
			a.add(LogField.os);
			a.add(LogField.time_zone);
			a.add(LogField.exchange_user_id);
			a.add(LogField.wtp_user_id);
			a.add(LogField.view_count);
			a.add(LogField.no_flash);
			a.add(LogField.age);
			a.add(LogField.gender);
			a.add(LogField.ethnicity);
			a.add(LogField.marital);
			a.add(LogField.kids);
			a.add(LogField.hhi);
			a.add(LogField.is_test);
			a.add(LogField.load_i5m);
			a.add(LogField.load_i9h);
			a.add(LogField.no_sync);
			a.add(LogField.mobile_carrier);
			a.add(LogField.mobile_loc);
			a.add(LogField.mobile_device_id);
			a.add(LogField.mobile_device_platform_id);
			a.add(LogField.mobile_device_make);
			a.add(LogField.mobile_device_model);
			a.add(LogField.mobile_device_js);
			a.add(LogField.conversion_type);
			a.add(LogField.adnexus_reserve_price);
			a.add(LogField.adnexus_estimated_clear_price);
			a.add(LogField.adnexus_estimated_average_price);
			a.add(LogField.adnexus_estimated_price_verified);
			a.add(LogField.adbrite_current_time);
			a.add(LogField.adbrite_previous_page_view_time);
			return a;
		}
	}
	
	
	private static class Version12
	{	
		
		public LogVersion getVersion()
		{
			return LogVersion.v12;	
		}
		
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
			List<LogField> nblist = Util.vector();
			nblist.addAll(getStandardHeader());
			nblist.addAll(getBidContext());
			popMapFromList(LogType.no_bid_all, LogVersion.v12, nblist);
		}
		
		void initBidAll()
		{
			List<LogField> balist = Util.vector();
			balist.addAll(getStandardHeader());
			balist.addAll(getBidContext());
			popMapFromList(LogType.bid_all, LogVersion.v12, balist);
		}
		
		void initImp()
		{
			List<LogField> implist = Util.vector();
			implist.addAll(getStandardHeader());
			implist.add(LogField.uuid);
			implist.add(LogField.winner_price);
			implist.addAll(getBidContext());
			popMapFromList(LogType.imp, LogVersion.v12, implist);
		}		
		
		void initClick()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.redirect_url);
			xlist.addAll(getBidContext());
			popMapFromList(LogType.click, LogVersion.v12, xlist);
		}				
		
		void initConversion()
		{
			List<LogField> xlist = Util.vector();
			xlist.addAll(getStandardHeader());
			xlist.add(LogField.uuid);
			xlist.add(LogField.conversion_id);
			xlist.addAll(getBidContext());
			xlist.add(LogField.is_post_view);
			xlist.add(LogField.is_post_click);
			popMapFromList(LogType.conversion, LogVersion.v12, xlist);
		}			
		
		public static List<LogField> getStandardHeader()
		{
			List<LogField> mlist = new Vector<LogField>();
			
			mlist.add(LogField.date_time);
			mlist.add(LogField.ip);
			mlist.add(LogField.country);
			mlist.add(LogField.region);
			mlist.add(LogField.request_uri);
			mlist.add(LogField.referer);
			mlist.add(LogField.useragent);
			
			return mlist;
		}
		
		public static List<LogField> getBidContext()
		{
			List<LogField> mlist = new Vector<LogField>();
			
			mlist.add(LogField.ad_exchange);
			mlist.add(LogField.auction_id);
			mlist.add(LogField.impression_id);
			mlist.add(LogField.bid);
			mlist.add(LogField.advertiser_id);
			mlist.add(LogField.campaign_id);
			mlist.add(LogField.line_item_id);
			mlist.add(LogField.line_item_type);
			mlist.add(LogField.creative_id);
			mlist.add(LogField.size);
			mlist.add(LogField.adaptv_creative_type);
			mlist.add(LogField.url);
			mlist.add(LogField.domain);
			mlist.add(LogField.url_keywords);
			mlist.add(LogField.referrer_url);
			mlist.add(LogField.referrer_domain);
			mlist.add(LogField.referrer_url_keywords);
			mlist.add(LogField.visibility);
			mlist.add(LogField.tag_format);
			mlist.add(LogField.within_iframe);
			mlist.add(LogField.google_adslot_id);
			mlist.add(LogField.google_verticals);
			mlist.add(LogField.google_main_vertical);
			mlist.add(LogField.google_main_vertical_with_weight);
			mlist.add(LogField.google_verticals_slicer);
			mlist.add(LogField.google_anonymous_id);
			mlist.add(LogField.admeld_website_id);
			mlist.add(LogField.admeld_publisher_id);
			mlist.add(LogField.admeld_tag_id);
			mlist.add(LogField.adnexus_tag_id);
			mlist.add(LogField.adnexus_inventory_class);
			mlist.add(LogField.openx_website_id);
			mlist.add(LogField.openx_placement_id);
			mlist.add(LogField.openx_category_tier1);
			mlist.add(LogField.openx_category_tier2);
			mlist.add(LogField.rubicon_website_id);
			mlist.add(LogField.rubicon_site_channel_id);
			mlist.add(LogField.rubicon_site_name);
			mlist.add(LogField.rubicon_domain_name);
			mlist.add(LogField.rubicon_page_url);
			mlist.add(LogField.improve_digital_website_id);
			mlist.add(LogField.yahoo_buyer_line_item_id);
			mlist.add(LogField.yahoo_exchange_id);
			mlist.add(LogField.yahoo_pub_channel);
			mlist.add(LogField.yahoo_seller_id);
			mlist.add(LogField.yahoo_seller_line_item_id);
			mlist.add(LogField.yahoo_section_id);
			mlist.add(LogField.yahoo_segment_id);
			mlist.add(LogField.yahoo_site_id);
			mlist.add(LogField.adbrite_zone_id);
			mlist.add(LogField.adbrite_zone_url);
			mlist.add(LogField.adbrite_zone_quality);
			mlist.add(LogField.adaptv_is_top);
			mlist.add(LogField.adaptv_placement_name);
			mlist.add(LogField.adaptv_placement_id);
			mlist.add(LogField.adaptv_placement_topics);
			mlist.add(LogField.adaptv_placement_quality);
			mlist.add(LogField.adaptv_placement_metrics);
			mlist.add(LogField.adaptv_video_id);
			mlist.add(LogField.casale_website_channel_id);
			mlist.add(LogField.casale_website_id);
			mlist.add(LogField.cox_content_categories);
			mlist.add(LogField.contextweb_categories);
			mlist.add(LogField.contextweb_tag_id);
			mlist.add(LogField.adjug_publisher_id);
			mlist.add(LogField.adjug_site_id);
			mlist.add(LogField.dbh_publisher_id);
			mlist.add(LogField.dbh_site_id);
			mlist.add(LogField.dbh_section_id);
			mlist.add(LogField.nexage_content_categories);
			mlist.add(LogField.nexage_publisher_id);
			mlist.add(LogField.nexage_site_id);
			mlist.add(LogField.user_ip);
			mlist.add(LogField.user_country);
			mlist.add(LogField.user_region);
			mlist.add(LogField.user_DMA);
			mlist.add(LogField.user_city);
			mlist.add(LogField.user_postal);
			mlist.add(LogField.user_language);
			mlist.add(LogField.language);
			mlist.add(LogField.user_agent);
			mlist.add(LogField.browser);
			mlist.add(LogField.os);
			mlist.add(LogField.time_zone);
			mlist.add(LogField.exchange_user_id);
			mlist.add(LogField.wtp_user_id);
			mlist.add(LogField.view_count);
			mlist.add(LogField.no_flash);
			mlist.add(LogField.age);
			mlist.add(LogField.gender);
			mlist.add(LogField.ethnicity);
			mlist.add(LogField.marital);
			mlist.add(LogField.kids);
			mlist.add(LogField.hhi);
			mlist.add(LogField.is_test);
			mlist.add(LogField.load_i5m);
			mlist.add(LogField.load_i9h);
			mlist.add(LogField.no_sync);
			mlist.add(LogField.mobile_carrier);
			mlist.add(LogField.mobile_loc);
			mlist.add(LogField.mobile_device_id);
			mlist.add(LogField.mobile_device_platform_id);
			mlist.add(LogField.mobile_device_make);
			mlist.add(LogField.mobile_device_model);
			mlist.add(LogField.mobile_device_js);
			mlist.add(LogField.conversion_type);
			mlist.add(LogField.adnexus_reserve_price);
			mlist.add(LogField.adnexus_estimated_clear_price);
			mlist.add(LogField.adnexus_estimated_average_price);
			mlist.add(LogField.adnexus_estimated_price_verified);
			mlist.add(LogField.adbrite_current_time);
			mlist.add(LogField.adbrite_previous_page_view_time);
			
			return mlist;
		}		
		

	}
}
