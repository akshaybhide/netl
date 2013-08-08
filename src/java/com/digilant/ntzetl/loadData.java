
package com.digilant.ntzetl;

import java.io.*;
import java.util.*;
import java.math.BigInteger;
import java.lang.Integer;
import java.text.SimpleDateFormat;
import java.security.*;
import java.util.regex.*;

import com.adnetik.shared.*;
import com.adnetik.shared.DbUtil.NZConnSource;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.BleStructure.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;
import com.digilant.mobile.MobileUtil;
import com.digilant.dbh.IABMap;
import com.digilant.dbh.IABMap.*;
import com.digilant.ntzetl.SendMail.*;


public class loadData
{
    private Writer _curWriter;
    private static ClickChecker _ccheck;
    private String _day;
    private int _okayClicks = 0;
    private int _failClicks = 0;
    private static int rownum = 0;
    private int failed = 0;
    private static int s = 0;
    private static int timeerror = 0;
    private SendMail _logMail;
    private int _curBatchCount = 0;

    // why doesn't System.getenv(<VAR>) work for these?
    private static String NZ_HOST = "66.117.49.50";
    private static String NZ_DATABASE = "fastetl";
    private static String NZ_USER = "jaffer";
    private static String NZ_PASSWORD = "jaffer_101?";

    private static LogType ltp;

    private static ExcName exchange;

    public loadData(String adex)
    {
	//_day=daycode;
	_ccheck = new ClickChecker();
	_logMail = new SendMail("NZloadReport for "+adex);
    }
    public void sendlogmail()
    {
	if ( failed>=4)
	    _logMail.send2admin();
    }

    private void openIfNecessary(String tsv_path) throws IOException
    {
	if (_curWriter == null) {
	    _curWriter = FileUtils.getWriter(tsv_path);
	}
    }

    public boolean createSubFile(String nfslogpath, String path) throws Exception
    {
	Random jrand = new Random();
	boolean read=false;
	try {
	    BufferedReader bread = FileUtils.getGzipReader(nfslogpath);

	    // Open the writer object
	    openIfNecessary(path);
	    int onefilecount = 0;
	    PathInfo pinfo = new PathInfo(nfslogpath);
	    _day = pinfo.getDate();
	    exchange = pinfo.pExc;
	    ltp = pinfo.pType;
	    int version = Integer.parseInt(pinfo.pVers.toString().substring(1,pinfo.pVers.toString().length()));

	    //	Util.massert(pinfo.pType == ltp,
	    //			"PathInfo says type is %s, but ltp is %s", pinfo.pType, ltp);

	    for (String oneline = bread.readLine(); oneline != null; oneline = bread.readLine()) {
		if (oneline.length()>0) {
		    try {
			// randomly sample 1/64 of bid_all records
			if (ltp==LogType.bid_all && 0 != jrand.nextInt(64)) continue;

			BidLogEntry ble = new BidLogEntry(ltp, pinfo.pVers, oneline);

			if (ltp == LogType.click) {
			    if (!_ccheck.greenLight(ble)) {
				_failClicks++;
				continue;
			    }
			    _ccheck.add2dataSet(ble);
			    _okayClicks++;
			}

			String cookieID = ble.getField(LogField.wtp_user_id);

			if (ltp==LogType.bid_all) {
			    if (cookieID.length()<36) cookieID = ble.getField(LogField.transaction_id);
			    if (cookieID.length()<36) {
				failed++;
				_logMail.pf("%s, %s, %s, %d records inserted, %d failed, cookieID: %s\n",
					    _day, exchange.toString(), ltp, rownum, failed, cookieID);
				/*_logMail.send2admin();*/
				continue;
			    }
			} else if (ltp==LogType.no_bid) {
			    if (cookieID.length()<36) cookieID = ble.getField(LogField.transaction_id);
			    if (cookieID.length()<36) {
				failed++;
				_logMail.pf("%s, %s, %s, %d records inserted, %d failed, cookieID: %s\n",
					    _day, exchange.toString(), ltp, rownum, failed, cookieID);
				/*_logMail.send2admin();*/
				continue;
			    }
			} else {
			    if (cookieID.length()<36) cookieID = ble.getField(LogField.uuid);
			    if (cookieID.length()<36) cookieID = ble.getField(LogField.transaction_id);
			    if (cookieID.length()<36) {
				failed++;
				_logMail.pf("%s, %s, %s, %d records inserted, %d failed, cookieID: %s\n",
					    _day, exchange.toString(), ltp, rownum, failed, cookieID);
				/*_logMail.send2admin(); */
				continue;
			    }
			}

			List<String> onerow = Util.vector();

			onerow.add(ble.getField(LogField.date_time));
			onerow.add(_day);
			// System.out.println(ble.getField(LogField.date_time).substring(0,10));

			onerow.add(pinfo.pVers.toString().substring(1,pinfo.pVers.toString().length()));
			onerow.add(ltp.toString());
			//System.out.println(ltp.toString());

			//Util.pf("Content field is %s,%d\n", ble.getField(LogField.content),ble.getFieldId(LogField.content));

			writewtp(onerow, cookieID);
			add2row(onerow, ble, LogField.ad_exchange, 15);
			add2row(onerow, ble, LogField.advertiser_id, 15);
			if (ble.getField(LogField.campaign_id).length()==0)
			    onerow.add("0");
			else add2row(onerow, ble, LogField.campaign_id, 10);
			if (ble.getField(LogField.line_item_id).length()==0)
			    onerow.add("0");
			else add2row(onerow, ble, LogField.line_item_id, 20);
			add2row(onerow, ble, LogField.creative_id, 20);
			add2row(onerow, ble, LogField.size, 10);
			add2row(onerow, ble, LogField.utw, 10);
			add2row(onerow, ble, LogField.content, 10);
			//System.out.println("content is "+ble.getField(LogField.content));
			//********
			String visible = ble.getField(LogField.visibility);
			if (visible.contains("NO")) onerow.add("null");
			else if (visible.contains("AB")) onerow.add("+1");
			else if (visible.contains("BE")) onerow.add("-1");
			//********
			add2row(onerow, ble, LogField.domain, 255);
			add2row(onerow, ble, LogField.google_adslot_id, 255);
			//add2row(onerow, ble, LogField.user_DMA, 10);
			onerow.add(String.valueOf(ble.getIntField(LogField.user_DMA)));
			//System.out.println(ble.getIntField(LogField.user_DMA));
			add2row(onerow, ble, LogField.user_city, 100);
			add2row(onerow, ble, LogField.user_postal, 10);
			add2row(onerow, ble, LogField.facebook_page_type, 10);

			if (ltp==LogType.bid_all || ltp==LogType.no_bid)
			    add2row(onerow, ble, LogField.user_ip, 23);
			else {
			    if (ble.getField(LogField.ip).length() > 0) {
				add2row(onerow, ble, LogField.ip, 23);
			    } else {
				add2row(onerow, ble, LogField.user_ip, 23);
			    }
			}

			if (ble.getField(LogField.language).length() > 0)
			    add2row(onerow, ble, LogField.language, 40);
			else add2row(onerow, ble, LogField.user_language, 40);
			add2row(onerow, ble, LogField.user_region, 40);
			add2row(onerow, ble, LogField.user_country, 2);

			if (ble.getField(LogField.ua_browser).length() > 0)
			    add2row(onerow, ble, LogField.ua_browser, 20);
			else add2row(onerow, ble, LogField.browser, 20);

			add2row(onerow, ble, LogField.ua_browser_version, 20);

			if (ble.getField(LogField.ua_os).length() > 0)
			    add2row(onerow, ble, LogField.ua_os, 20);
			else add2row(onerow, ble, LogField.os, 20);

			add2row(onerow, ble, LogField.ua_os_version, 20);

			add2row(onerow, ble, LogField.ua_device_type, 20);

			if (ble.getField(LogField.ua_device_maker).length() > 0)
			    add2row(onerow, ble, LogField.ua_device_maker, 20);
			else add2row(onerow, ble, LogField.mobile_device_make, 20);

			if (ble.getField(LogField.ua_device_model).length() > 0)
			    add2row(onerow, ble, LogField.ua_device_model, 20);
			else add2row(onerow, ble, LogField.mobile_device_model, 20);

			add2row(onerow, ble, LogField.mobile_carrier, 20);

			add2row(onerow, ble, LogField.mobile_loc, 20);
			add2row(onerow, ble, LogField.mobile_device_js, 20);

			add2row(onerow, ble, LogField.is_mobile_app, 20);

			add2row(onerow, ble, LogField.dbh_publisher_id, 20);

			add2row(onerow, ble, LogField.dbh_site_id, 20);

			add2row(onerow, ble, LogField.dbh_placement_id, 20);

			add2row(onerow, ble, LogField.dbh_ad_tag_type, 20);

			add2row(onerow, ble, LogField.dbh_default_type, 20);

			//********cost, publisher_payout,currency, bid********
			if (ltp==LogType.imp) add2row(onerow, ble, LogField.winner_price, 20);
			else onerow.add("null");
			if (ltp==LogType.imp) add2row(onerow, ble, LogField.publisher_payout, 20);
			else onerow.add("null");
			add2row(onerow, ble, LogField.currency, 20);
			if (ltp==LogType.no_bid) onerow.add("null");
			else add2row(onerow, ble, LogField.bid, 20);


			// lzcnt -- number of consecutive low-order zeros in random number.

			// if (ltp==LogType.click)
			//     onerow.add("-3");
			// else if (ltp==LogType.conversion)
			//     onerow.add("-2");
			// else if (ltp==LogType.activity)
			//     onerow.add("-1");

                        if (ltp==LogType.click) onerow.add("19");
                        else if (ltp==LogType.activity) onerow.add("18");
                        else if (ltp==LogType.conversion) onerow.add("17");
                        else {
                            int i = (int)Math.floor(Math.log(7.629452739355006e-6
                                                             + 999.9923705472606e-3
                                                             * Math.random())
                                                    / Math.log(.5));
                            onerow.add(String.valueOf(i));
                        }


			//********

			// deal with is_post_click,is_post_view, pixel_ID

			if (ltp==LogType.conversion || ltp==LogType.activity) {
			    add2row(onerow, ble, LogField.is_post_click, 20);
			    add2row(onerow, ble, LogField.is_post_view, 20);
			    add2row(onerow, ble, LogField.conversion_id, 20);
			} else {
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			}

			//********param0 to param 10 ********
			String val;
			String[] wtpi;
			if (ble.hasField("dbh_macro")) {
			    int fid = ble.getFieldId(LogField.dbh_macro);
			    int fcount = ble.getFieldCount();
			    val = (fid >= fcount ? "" : ble.getField(LogField.dbh_macro));
			} else val = "";
			if (val=="") {
			    wtpi = new String[1];
			    wtpi[0] = "";
			} else wtpi = val.split("\\|");
			for (int k = 1; k <= 10; k++) {
			    boolean kwasthere = false;
			    int l;
			    if (k == 4) l = 399; else l = 299;
			    for (String wtp:wtpi) {
				if (wtp.equals("")) break;
				if (wtp.contains("=") && wtp.substring(0, wtp.indexOf("=")).contains(k+"")) {
				    String v = wtp.substring(wtp.indexOf("=")+1);
				    // valpart.append(MobileUtil.encloseInSingleQuotes(v.substring(0, Math.min(l, v.length()))));
				    //  String parameter = MobileUtil.encloseInSingleQuotes(v.substring(0, Math.min(l, v.length())));
				    String parameter = v.substring(0, Math.min(l, v.length()));
				    // onerow.add(MobileUtil.encloseInSingleQuotes(v.substring(0, Math.min(l, v.length()))));
				    // System.out.println(MobileUtil.encloseInSingleQuotes(v.substring(0, Math.min(l, v.length()))));
				    parameter = (parameter.length() > 200 ? parameter.substring(0, 200) : parameter);
				    onerow.add(parameter);
				    kwasthere = true;
				    break;
				}
			    }
			    if (!kwasthere) onerow.add("null");
			}


			//******** end of param0 to param 10 ********

			//****yahoo and rubicon
			if (exchange==ExcName.admeta || exchange==ExcName.nexage || exchange==ExcName.liveintent) {
			    String diab = "";
			    if (exchange==ExcName.admeta) diab = ble.getField(LogField.admeta_site_categories);
			    else if (exchange==ExcName.nexage) diab = ble.getField(LogField.nexage_content_categories);
			    else if (exchange==ExcName.liveintent) diab = ble.getField(LogField.liveintent_site_categories);

			    if (diab.contains(",")) diab = diab.substring(0,diab.indexOf(","));

			    if (diab.length()==0)   onerow.add("null");
			    else onerow.add(diab);
			} else if (exchange==ExcName.yahoo || exchange==ExcName.rubicon) {
			    String direct = ble.getField(LogField.iab_category);
			    if (direct.contains(",")) direct = direct.substring(0,direct.indexOf(","));
			    if (direct.contains("nil")) direct = "IAB24";
			    else if (direct.length()>0) direct = "IAB"+direct;
			    if (direct.length()==0) onerow.add("null");
			    else onerow.add(direct);
			} else if (exchange==ExcName.rtb || exchange==ExcName.openx || exchange==ExcName.contextweb) {
			    if (version >= 22) {
				IABMap mapper = IABMap.getInstance();
				String segid = ble.getField(IABMap.getCategoryLogField(Util.getExchange(nfslogpath)));
				String iab;
				if (segid.contains(","))
				    iab = mapper.Lookup(exchange, segid.substring(0, segid.indexOf(",")));
				else iab = mapper.Lookup(exchange,segid);
				if (iab.length()>0) {
				    if (iab.contains(","))
					onerow.add(iab.substring(0,iab.indexOf(",")));
				    else onerow.add(iab);
				}
				else onerow.add("null");
			    }
			    else onerow.add("null");
			}
			else onerow.add("null");
			add2row(onerow, ble, LogField.iab_category, 10);

			if (ble.getField(LogField.time_zone).contains(".")) {
			    if (ble.getField(LogField.time_zone).contains("(")) {timeerror++; onerow.add("null");}
			    else {
				float tz = Float.parseFloat(ble.getField(LogField.time_zone));
				int timez = (int)Math.round(tz);
				onerow.add(String.valueOf(timez));
			    }
			    if (timeerror>5)
				_logMail.pf("%s, %s, %s, %d records inserted, time_zone has 5 or more errors",
					    _day, exchange.toString(), ltp, rownum);
			    // _logMail.send2admin(); continue;}
			} else
			    add2row(onerow, ble, LogField.time_zone, 6);

			// deal with user_agent, cookie born,cpa_amount FLOAT,top_10_segments_flag,bid_floor_cpm FLOAT,loss_reason_code

			if (version >= 22) {   // user_agent
			    String uag = ble.getField(LogField.user_agent_hash);
			    if (uag.length() >= 32) {
				MessageDigest m = MessageDigest.getInstance("MD5");
				m.update(uag.getBytes("UTF-8"),0,uag.length()>16? 16:uag.length());
				byte[] mhash;
				mhash = m.digest();
				long hash = 0L;
				for (int i = 0;i<8;i++) {
				    hash = hash << 8 | mhash[i] & 0x00000000000000FFL;
				}
				onerow.add(String.valueOf(hash));
			    }
			    else {onerow.add("null");}

			    // cookie_born
			    long imptime;
			    if (ble.getField(LogField.cookie_born).length()>0) {
				imptime = Integer.parseInt(ble.getField(LogField.cookie_born));
				//System.out.println(imptime);
				//java.util.Date time = new java.util.Date((long)imptime*1000);System.out.println(time);
				Calendar mydate = Calendar.getInstance();
				mydate.setTimeInMillis(imptime*1000);
				//System.out.println(mydate.get(Calendar.DAY_OF_MONTH)+"."+mydate.get(Calendar.MONTH)+"."+mydate.get(Calendar.YEAR));
				int month=mydate.get(Calendar.MONTH);
				month=month+1;
				String trantime = mydate.get(Calendar.YEAR)+"-"+month+"-"+mydate.get(Calendar.DAY_OF_MONTH);

				onerow.add(trantime);
			    }
			    else onerow.add("null");
			    //add2row(onerow, ble, LogField.cookie_born, 20); System.out.println(ble.getField(LogField.cookie_born));

			    //cpa_amount
			    add2row(onerow, ble, LogField.cpa_amount, 20);
			    // top_10_segments_flag
			    add2row(onerow, ble, LogField.top_10_segments_flag, 20);
			    //bid_floor_cpm
			    add2row(onerow, ble, LogField.bid_floor_cpm, 20);
			} else {
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			}

			// tag_id CHARACTER VARYING(12), -- from admeld_tag_id, adnexus_tag_id, contextweb_tag_id ???

			if (exchange==ExcName.admeld)
			    //String tagid = ble.getField(LogField.admeld_tag_id);
			    add2row(onerow, ble, LogField.admeld_tag_id, 30);
			//System.out.println("admeld"+ble.getField(LogField.admeld_tag_id));}
			else if (exchange==ExcName.adnexus)
			    add2row(onerow, ble, LogField.adnexus_tag_id, 30);
			else if (exchange==ExcName.contextweb)
			    add2row(onerow, ble, LogField.contextweb_tag_id, 30);
			else if (exchange==ExcName.pubmatic)
			    add2row(onerow, ble, LogField.pubmatic_ad_id, 30);
			else onerow.add("null");
			//System.out.println(ble.getField(LogField.contextweb_tag_id));

			add2row(onerow, ble, LogField.publisher_pricing_type, 20);
			add2row(onerow, ble, LogField.advertiser_pricing_type, 20);
			// System.out.println(ble.getField(LogField.advertiser_pricing_type));
			String within = ble.getField( LogField.within_iframe);
			if (within.length()>4) onerow.add("null");
			else if (within.equals("true")) onerow.add("1");
			else if (within.equals("false")) onerow.add("0");
			else if (within.equals("1") || within.equals("0")) onerow.add(within);
			else onerow.add("null");
			//add2row(onerow, ble, LogField.within_iframe, 20);
			//System.out.println(ble.getField(LogField.within_iframe));
			add2row(onerow, ble, LogField.retargeting_timelimit, 20);
			//if (ble.getField(LogField.retargeting_timelimit)==null)
			//  System.out.println("timelimit"+ble.getField(LogField.retargeting_timelimit));

			if (version >= 22 && ltp==LogType.conversion) {
			    add2row(onerow, ble, LogField.conversion_interval, 20);
			    //System.out.println(ble.getField(LogField.conversion_interval));
			}
			else {
			    onerow.add("null");
			}
			if (version >= 22) {
			    add2row(onerow, ble, LogField.targeting_criteria, 20);
			} else {
			    onerow.add("null");
			}

			if (exchange==ExcName.adnexus) {
			    add2row(onerow, ble, LogField.adnexus_reserve_price, 20);
			    //System.out.println(ble.getField(LogField.adnexus_reserve_price));
			    add2row(onerow, ble, LogField.adnexus_estimated_clear_price, 20);
			    add2row(onerow, ble, LogField.adnexus_estimated_average_price, 20);
			    add2row(onerow, ble, LogField.adnexus_estimated_price_verified, 20);
			}
			else {
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			}
			if (version >= 22) {
			    long imptime;
			    if (ble.getField(LogField.impression_timestamp).length()>0)
				{imptime = Integer.parseInt(ble.getField(LogField.impression_timestamp));
				    // System.out.println(imptime);
				    //java.util.Date time = new java.util.Date((long)imptime*1000);System.out.println(time);
				    Calendar mydate = Calendar.getInstance();
				    mydate.setTimeInMillis(imptime*1000);
				    int month=mydate.get(Calendar.MONTH);
				    month=month+1;
				    String time=mydate.get(Calendar.HOUR_OF_DAY)+":"+mydate.get(Calendar.MINUTE)+":"+mydate.get(Calendar.SECOND);
				    String trantime =mydate.get(Calendar.YEAR)+"-"+month+"-"+mydate.get(Calendar.DAY_OF_MONTH)+" "+time;
				    onerow.add(trantime);
				}
			    else onerow.add("null");
			}
			else onerow.add("null");

			//********mobile**mobile********
			String mobile = ble.getField(LogField.mobile_device_id);
			if (mobile.length() > 0) {
			    MessageDigest m = MessageDigest.getInstance("MD5");
			    m.update(mobile.getBytes("UTF-8"),0,mobile.length()>16? 16:mobile.length());
			    byte[] mhash;
			    mhash = m.digest();
			    long hash = 0L;
			    for (int i = 0;i<8;i++) {
				hash = hash << 8 | mhash[i] & 0x00000000000000FFL;
			    }
			    onerow.add(String.valueOf(hash));
			} else
			    onerow.add("null");

			String mobile_platform = ble.getField(LogField.mobile_device_platform_id);
			//System.out.println("mobile is:"+mobile);
			if (mobile_platform.length()>0) {
			    MessageDigest m = MessageDigest.getInstance("MD5");
			    byte[] mhash;
			    long hash = 0L;
			    m.update(mobile_platform.getBytes("UTF-8"),0,mobile_platform.length()>16? 16:mobile_platform.length());
			    mhash = m.digest();
			    for (int i = 0;i < 8; i++) {
				hash = hash << 8 | mhash[i] & 0x00000000000000FFL;
			    }
			    onerow.add(String.valueOf(hash));
			} else
			    onerow.add("null");

			if (version >= 23) {
			    if (ltp==LogType.no_bid_all || ltp==LogType.imp) {
				onerow.add("null");
				onerow.add("null");
				onerow.add("null");
			    } else {
				onerow.add("null");
				onerow.add("null");
				onerow.add("null");
			    }
			} else {
			    onerow.add("null");
			    onerow.add("null");
			    onerow.add("null");
			}
			if (version >= 22 && ltp==LogType.no_bid_all)
			    add2row(onerow, ble, LogField.random_prefiltered_user, 10);
			else onerow.add("null");

			add2row(onerow, ble, LogField.deal_price, 20);
			if (exchange==ExcName.adnexus)
			    add2row(onerow, ble, LogField.adnexus_inventory_class, 15);
			else onerow.add("null");
			if (exchange==ExcName.rubicon)
			    add2row(onerow, ble, LogField.rubicon_site_channel_id, 15);
			else if (exchange==ExcName.casale)
			    add2row(onerow, ble, LogField.casale_website_channel_id, 15);
			else onerow.add("null");

			if (ltp==LogType.bid_all)
			    onerow.add("6");
			else onerow.add("0");
			//********

			FileUtils.writeRow(_curWriter, "\t", onerow.toArray());
			rownum++;
			_curBatchCount++;
			onefilecount++;
			read=true;
		    } // try

		    catch (Exception ex ) {
			System.out.println(ex);
			failed++;
			_logMail.pf("%s, %s, %s, %d records inserted, %d failed, exception is :%s\n",
				    _day, exchange.toString(), ltp, rownum, failed, ex);
			continue;
		    }
		} // if length >0
	    }
	    bread.close();
	    return read;
	}
	catch (IOException ex ) {
	    failed++;
	    _logMail.pf("%s, %s, %s, %d records inserted, %d failed, log path dropped: %s\n",
			_day, exchange.toString(), ltp, rownum, failed, ex);
	    return read;

	}
    }

    public int CountBatch()
    {
	return _curBatchCount;
    }
    static void writewtp(List<String> onerow, String wtpid) throws IOException
    {
	List <BigInteger> wtp = wtptrans(wtpid);
	//System.out.print("left"+String.valueOf(wtp.get(0)));//System.out.print("right"+wtp.get(1));
	String left = String.valueOf(wtp.get(0)); //System.out.println(left);
	String right = String.valueOf(wtp.get(1));//System.out.println(right);
	//left = (left.length() > 40 ? left.substring(0, 40) : left);
	//right = (right.length() > 40 ? right.substring(0, 40) : right);
	onerow.add(left); onerow.add(right);
    }

    static List<BigInteger> wtptrans(String cookieID)
    {
	List<BigInteger> list = Util.vector();
	String number = "";
	while (cookieID.indexOf("-") != -1) {
	    int i = cookieID.indexOf("-");
	    String test = cookieID.substring(0,i);
	    cookieID = cookieID.substring(i + 1, cookieID.length());
	    number += test;
	}
	number += cookieID;
	BigInteger left = new BigInteger(number.substring(0, 16), 16);
	BigInteger right = new BigInteger(number.substring(16, 32), 16);
	left = left.subtract(BigInteger.ONE.shiftLeft(63));
	right = right.subtract(BigInteger.ONE.shiftLeft(63));
	list.add(left);
	list.add(right);
	return list;
    }

    static void add2row(List<String> onerow, BidLogEntry ble, LogField lf, int maxsize)
    {
	String fval = ble.getField(lf);
	fval = (fval.length() > maxsize ? fval.substring(0, maxsize) : fval);
	onerow.add(fval);
    }
    public void transferNLoad(String tsv_path,String tabname) throws IOException
    {
	int k=0;
	if (_curWriter != null) {
	    _curWriter.close();
	    _curWriter = null;
	}

	File upfile = new File(tsv_path);
	if(upfile.length()>0)
	    {
		boolean load=LoadCommand(tsv_path,tabname);
		while(load==true )
		    {
			load=LoadCommand(tsv_path,tabname);
			k++;
			if (k==5)
			    _logMail.send2admin();
		    }
		if (load==false) removeCommand(tsv_path);
	    }

	_curBatchCount = 0;
    }

    public boolean LoadCommand(String remnzpath, String tabname) throws IOException
    {
	boolean errs = false;
	String nzload = Util.sprintf("nzload -host %s -u %s -pw %s -db %s -t %s -maxErrors 5 -df %s",
				     NZ_HOST,
				     NZ_USER,
				     NZ_PASSWORD,
				     NZ_DATABASE,
				     tabname,
				     remnzpath);
	// System.out.println(nzload);
	//_logMail.pf("Initiating disk-NZ load...\n");

	List<String> outlist = Util.vector();
	List<String> errlist = Util.vector();

	double startup = Util.curtime();
	boolean load=Util.syscall(nzload, outlist, errlist);

	//logmail.pf(" ... done, %d output lines, %d error lines, took %.03f\n",
	//	outlist.size(), errlist.size(), (Util.curtime()-startup)/1000);
	if(load==true)
	    for (String oneerr : errlist) {
		//logmail.pf("ERROR: %s\n", oneerr);
		_logMail.pf("\n%s, %s, %d records inserted, %d failed, nzload says:\n%s\n",
			    _day, exchange, rownum, failed, oneerr);
		errs = true;
		if (oneerr.contains("Permission"))
			System.exit(1);
	    }
	return errs;
    }

    private void removeCommand(String remnzpath) throws IOException
    {
	String removefile = Util.sprintf("rm %s", remnzpath);
	String syscall = Util.sprintf("%s", removefile);

	List<String> outlist = Util.vector();
	List<String> errlist = Util.vector();

	Util.syscall(syscall, outlist, errlist);
	for (String oneerr : errlist)
	    {
		_logMail.pf("%s, %s, %s, remove-file error: %s\n",
			    _day, exchange, ltp, oneerr);
	    }
    }
}
