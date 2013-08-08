package com.digilant.dbh;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import com.adnetik.shared.BidLogEntry;
import com.adnetik.shared.BidLogEntry.BidLogFormatException;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.PathInfo;
import com.adnetik.shared.Util;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.Util.LogField;
import com.digilant.fastetl.FileManager;
import com.digilant.fastetl.FastUtil.MyLogType;
public class IABMap
{
    private TreeMap<ExcName, TreeMap<String, String>> _seg_map;
    private String mapfile = System.getenv("HOME")+"/src/resources/IAB_map.txt";
    public static IABMap iabinstance;
    private IABMap() {
	_seg_map = new TreeMap<ExcName, TreeMap<String, String>>();
	populateMap();
    }
    public static IABMap getInstance() {
	if (iabinstance==null) init();
	return iabinstance;
    }
    private static void init() {
	iabinstance = new IABMap();
    }
    public String Lookup(ExcName exname, String segid ) {
	if (!_seg_map.containsKey(exname)) return "NULL";
	if (!_seg_map.get(exname).containsKey(segid)) return "IAB24";
	//System.out.println(_seg_map.get(ExcName.yahoo));
	return _seg_map.get(exname).get(segid);
    }
    private void populateMap() {
	Scanner sc;
	try {
	    sc = new Scanner(new File(mapfile));
	    while(sc.hasNext()) {
		String iab_id = sc.next();
		String seg_id = sc.next();
		String ex_name = sc.next();
		//System.out.println(iab_id + " " + seg_id+" "+ ex_name);
		ExcName key = ExcName.valueOf(ex_name);
		if (_seg_map.containsKey(key)) {
		    _seg_map.get(key).put(seg_id, iab_id);
		}
		else {
		    TreeMap<String, String> tmp = new TreeMap<String, String>();
		    tmp.put(seg_id, iab_id);
		    _seg_map.put(key, tmp);
		}
		//System.out.println(iab_id + ","+seg_id + "," + ex_name);
	    }
	    sc.close();
	} catch (FileNotFoundException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
    public static LogField getCategoryLogField(ExcName ename) {
	if (ename==ExcName.openx)
	    return LogField.openx_category_tier1;
	if (ename==ExcName.contextweb)
	    return LogField.contextweb_categories;
	if (ename==ExcName.nexage)
	    return LogField.nexage_content_categories;
	if (ename==ExcName.admeta)
	    return LogField.admeta_site_categories;
	if (ename==ExcName.liveintent)
	    return LogField.liveintent_publisher_categories;
	if (ename==ExcName.rtb)
	    return LogField.google_main_vertical;
	if (ename==ExcName.liveintent)
	    return LogField.liveintent_site_categories;
	if (ename==ExcName.spotx)
	    return LogField.spotx_categories;
	return LogField.iab_category;
    }
    public static void main(String[] args) throws IOException {
	IABMap mapper =  IABMap.getInstance();
	System.out.println(mapper.Lookup(ExcName.yahoo, "17"));
	ExcName e = Util.getExchange("/mnt/adnetik/adnetik-uservervillage/admeld/userver_log/imp/2013-03-27/");
	System.out.println("exchange:" + e.toString());
	e = Util.getExchange("/mnt/adnetik/adnetik-uservervillage/yahoo/userver_log/imp/2013-04-26/");
	BufferedReader bread = null;
	try {
	    MyLogType[] mlt = new MyLogType[]{MyLogType.imp};
	    ExcName[] en = new ExcName[]{e};
	    FileManager _fileMan = new FileManager("/home/armita/mconfig");

	    Set<String> pathset = _fileMan.newFilesLookBack("2013-04-26", 1, mlt, en);
	    List<String> pathlist = new Vector<String>(pathset);
	    for (String f : pathlist) {
		bread = FileUtils.getGzipReader("/mnt/adnetik/adnetik-uservervillage/yahoo/userver_log/imp/2013-04-26/2013-04-26-01-30-00.EDT.imp_v22.yahoo-rtb-california1_35ef7.log.gz");
		PathInfo pinfo = new PathInfo("/mnt/adnetik/adnetik-uservervillage/yahoo/userver_log/imp/2013-04-26/2013-04-26-01-30-00.EDT.imp_v22.yahoo-rtb-california1_35ef7.log.gz");
		String iab;
		for (String oneline = bread.readLine(); oneline != null; oneline = bread.readLine()) {
		    BidLogEntry ble;
		    try {
			ble = new BidLogEntry(pinfo.pType, pinfo.pVers, oneline);
			String segid = ble.getField(IABMap.getCategoryLogField(Util.getExchange("/mnt/adnetik/adnetik-uservervillage/yahoo/userver_log/imp/2013-04-26/")));//System.out.println("test");
			if (segid.contains(","))
			    iab = mapper.Lookup(e, segid.substring(0, segid.indexOf(",")));
			else iab=mapper.Lookup(e,segid);
			if (iab.length()>0)
			    {
				if (iab.contains(","))
				    iab=iab.substring(0,iab.indexOf(","));
			    }
			//if (!iab.contains("IAB"))
			System.out.println(segid+"--> " +iab);
		    } catch (BidLogFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		    }
		}
	    }
	} catch (IOException e2) {
	    // TODO Auto-generated catch block
	    e2.printStackTrace();
	}
	//mapper.print();
    }
}
