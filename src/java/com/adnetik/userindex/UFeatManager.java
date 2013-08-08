
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;
import com.adnetik.userindex.UserFeature.*;

import com.adnetik.data_management.BluekaiDataMan;
import com.adnetik.data_management.ExelateDataMan;


public class UFeatManager
{
	
	private static UFeatManager _SING;
	
	// Set of positive requests for the week
	private SortedSet<PosRequest> _preqSet = Util.treeset();
	
	// This is a map of all the scan requests to their feature sets.
	// Each request has a separate copy of its feature functions - that's not too much weight
	private SortedMap<ScanRequest, SortedSet<UserFeature>> _featMap = Util.treemap();	
	
	private String _blockEndDay; 
	
	
		
	UFeatManager(String blockend) throws IOException
	{
		TimeUtil.assertValidDayCode(blockend);	
		Util.massert(UserIndexUtil.isBlockEndDay(blockend), 
			"Must be initialized with a block-end day, found %s", blockend);
		
		_blockEndDay = blockend;
		
		FileSystem fsys = FileSystem.get(new Configuration());
		
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, Util.sprintf("/userindex/featman/%s/*.txt", blockend));
		
		for(Path onepath : pathlist)
		{	
			String fname = onepath.getName();
			String listcode = fname.split("\\.")[0];
			// Util.pf("File name is %s, listcode is %s\n", fname, listcode);
			
			Util.massert(ListInfoManager.getSing().havePosRequest(listcode),
				"Listcode %s found in FEATMAN listing but not LISTCODE database", listcode);
			
			PosRequest preq = ListInfoManager.getSing().getPosRequest(listcode);
			Util.putNoDup(_preqSet, preq);
		}
		
		Util.pf("Found %d scan requests for block-end %s\n", 
			_preqSet.size(), _blockEndDay);
		
		/*
		for(PosRequest preq : _preqSet)
		{
			SortedSet<UserFeature> feat4req = readFeatSet(preq, blockend);
			_featMap.put(preq, feat4req);
			
			// Also add all the features from the positive request to the negative request
			NegRequest negreq = preq.getNegRequest();
			Util.setdefault(_featMap, negreq, new TreeSet<UserFeature>());
			_featMap.get(negreq).addAll(feat4req);
			
			Util.pf("Read %d features for request %s\n", 
				feat4req.size(), preq.getListCode());
		}
		
		for(ScanRequest screq : _featMap.keySet())
		{
			Util.pf("Found %d features for request %s\n", 
				_featMap.get(screq).size(), screq.getListCode());
		}
		*/
	}	
	
	public static synchronized boolean isSingReady()
	{
		return _SING != null;	
	}
	
	public static UFeatManager getSing()
	{
		Util.massert(_SING != null, "Must call initSing(...) first");
		return _SING;
	}
	
	public static synchronized void clearSing() 
	{
		_SING = null;
	}
	
	public static synchronized void initSing(String daycode) throws IOException
	{
		Util.massert(_SING == null, "Attempt to call initSing(..) but singleton already initialized");

		_SING = new UFeatManager(daycode);	
	}
	
	public static void buildFeatMan4Date(SimpleMail logmail, String daycode) throws IOException
	{
		TimeUtil.assertValidDayCode(daycode);
		Util.massert(UserIndexUtil.isBlockEndDay(daycode),
			"Daycode is not a block-end %s", daycode);
		
		FeatureCompiler fcomp = new FeatureCompiler(daycode);
		
		FileSystem fsys = FileSystem.get(new Configuration());
		
		for(PosRequest preq : ListInfoManager.getSing().getPosRequestSet())
		{
			if(preq.isActiveOn(daycode))
			{
				// logmail.pf("Going to add scan request %s to package\n", preq);
				
				SortedSet<UserFeature> feat4req = fcomp.featSet4PosReq(preq);
				SortedSet<String> keyset = Util.treeset();
				
				for(UserFeature ufeat : feat4req)
				{
					String namekey = ufeat.getNameKey();
					Util.putNoDup(keyset, namekey);
					
					checkCopy(ufeat);
				}
				
				logmail.pf("Found %d features for scan request %s\n", 
					feat4req.size(), preq.getListCode());
				
				HadoopUtil.writeObjectStr2Path(keyset, fsys, getFeatManPath(preq, daycode));
			}
		}
	}
	
	public boolean havePosRequest(PosRequest preq)
	{
		return _preqSet.contains(preq);
	}
	
	// Run the feature through the mapping to a Name Key
	private static void checkCopy(UserFeature ufeat)
	{
		String namekey = ufeat.getNameKey();
		UserFeature mycopy = UserFeature.buildFromNameKey(namekey);
		String keycopy = mycopy.getNameKey();
		
		Util.massert(namekey.equals(keycopy),
			"Name key is %s but copy is %s", 
			namekey, keycopy);
	}
	
	// Path to save file for the week
	private static String getFeatManPath(PosRequest preq, String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);	
		return Util.sprintf("/userindex/featman/%s/%s.txt", daycode, preq.getListCode());
	}
	
	public synchronized SortedSet<UserFeature> getFeatSet(ScanRequest scanreq)
	{
		if(!_featMap.containsKey(scanreq))
		{
			SortedSet<UserFeature> featset = Util.treeset();
			
			if(scanreq instanceof NegRequest)
			{
				NegRequest negreq = (NegRequest) scanreq;
				
				for(PosRequest preq : _preqSet)
				{
					// Recursive call, with memoization					
					if(preq.getNegRequest().equals(negreq))
						{ featset.addAll(getFeatSet(preq)); }
				}		
								
			} else {
				// Implied else-if
				PosRequest posreq = (PosRequest) scanreq;
				
				Util.massert(_preqSet.contains(posreq),
					"Feat List for %s does not contain positive request %s\n",
					_blockEndDay, posreq.getListCode());
				
				// Keep track of feat-load calls
				try { 
					SortedSet<UserFeature> reqset = readFeatSet(posreq, _blockEndDay); 
					featset.addAll(reqset);
				} catch (IOException ioex) { throw new RuntimeException(ioex); }
			}				
				
			_featMap.put(scanreq, featset);
		}
		

		return _featMap.get(scanreq);
	}
	
	// This should really be deprecated or something
	public Map<String, UserFeature> getReportFeatMap(ScanRequest screq)
	{
		Map<String, UserFeature> onemap = Util.treemap();
		
		for(UserFeature ufeat : getFeatSet(screq))
			{ onemap.put(ufeat.toString(), ufeat); }
		
		return onemap;
	}
	
	// Reads the feature set from HDFS
	private static SortedSet<UserFeature> readFeatSet(PosRequest preq, String blockend) throws IOException
	{
		String featpath = getFeatManPath(preq, blockend);
		List<String> featlist = HadoopUtil.readFileLinesE(featpath);
		
		SortedSet<UserFeature> featset = Util.treeset();
		for(String onefeat : featlist)
		{
			UserFeature thefeat = UserFeature.buildFromNameKey(onefeat);
			Util.putNoDup(featset, thefeat);
		}
		
		// Util.pf("Found %d features for pos request %s\n", 
		//	featset.size(), preq.getListCode());
		
		return featset;		
	}
	
	// This class does nothing but build lists of features for requests.
	// If you develop a new feature and want to add it to the set, put it here.
	static class FeatureCompiler
	{
		DomainAgg _domAgg;
		String _dayCode;
		
		FeatureCompiler(String dc)
		{
			TimeUtil.assertValidDayCode(dc);
			_dayCode = dc;
		}
		
		
		// These could potentially be moved to a different file or an inner subclass
		private SortedSet<UserFeature> featSet4PosReq(PosRequest preq)
		{
			SortedSet<UserFeature> reqset = Util.treeset();
			reqset.addAll(basicFeatureCompile(preq));
			reqset.addAll(pixelFeatureCompile(preq));
			return reqset;
		}
		
		private SortedSet<UserFeature> pixelFeatureCompile(PosRequest preq)
		{
			Util.massert(preq != null, "Attempt to compile features for null preq");
			
			SortedSet<UserFeature> pixfeatset = Util.treeset();
			
			if(preq instanceof ScanRequest.HasClientPixel)
			{
				Integer pixid = ((ScanRequest.HasClientPixel) preq).getAllowedPixel();

				// Could maybe be null, if there is a UsrMultiRequest with no rows in multipix table
				if(pixid != null)
				{	
					long clientid = AdBoardApi.getPacSing().getClientId4Pixel(pixid);
					Set<Integer> sibpixset = AdBoardApi.getPacSing().getPixelAccessList(clientid);
					
					for(Integer onesib : sibpixset)
						{ pixfeatset.add(new SimplePixelFeature(onesib)); }
				}
			}
			return pixfeatset;
		}
		
		private SortedSet<UserFeature> basicFeatureCompile(PosRequest preq)
		{
			List<UserFeature> featlist = Util.vector();
			// Okay this is where all the feature data goes 
			
			featlist.add(new NullFeat());
			
			// Hispanic Features by Quintile 
			for(int i = 0; i < 5; i++)
			{
				//double low = i;
				// double hgh = i+1;
				HispanicFeature hfeat = new HispanicFeature(i);
				featlist.add(hfeat);
			}		
			
			// Domains
			{
				if(_domAgg == null)
				{
					_domAgg = new DomainAgg(_dayCode);
					_domAgg.loadDomainInfo();
				}				
				
				for(Pair<Integer, String> dompair : _domAgg.getTopDomSet(preq.getCountryCode(), 500))
				{
					addSingleModePair(featlist, LogField.domain, dompair._2);
				}				
			}
			
			
			// TODO: shouldn't this be "user_hour"....?
			for(int hour = 0; hour < 24; hour++)
			{
				String phour = (hour < 10 ? "0" : "") + hour;
				
				addSingleModePair(featlist, LogField.hour, phour);
			}			
			
			// Surfing behavior type features
			for(int nt = 1; nt < 16; nt++)
			{
				addVarietyHitThresh(featlist, LogField.user_region, nt);
				addVarietyHitThresh(featlist, LogField.user_country, nt);
				addVarietyHitThresh(featlist, LogField.user_ip, nt);
				addVarietyHitThresh(featlist, LogField.hour, nt);
				addVarietyHitThresh(featlist, LogField.domain, nt);
				addVarietyHitThresh(featlist, LogField.google_main_vertical, nt);
			}		
			
			// Combined country_region features
			for(String s : FeatureInfo.getCountryRegionList())
			{
				addSingleModePair(featlist, LogField.country_region, s);
			}			
			
			
			// Google vertical feature list
			// TODO: this should go into FeatureInfo
			for(Integer s : getVertList())
			{
				featlist.add(new GoogVertFeat(s, true));
				featlist.add(new GoogVertFeat(s, false));
			}			
			
			// Browsers
			for(String s : getBrowsers())
			{
				featlist.add(new SingleMatch(LogField.browser, s));	
			}
			
			// Operating system
			for(String oscode : Util.OS_CODES)
			{
				addSingleModePair(featlist, LogField.os, oscode);
			}	
			
			// Exchange features
			for(ExcName exc : ExcName.values())
			{
				addSingleModePair(featlist, LogField.ad_exchange, exc.toString());
			}	
			
			// Ad Hoc domain categories
			{
				for(String onecat : FeatureInfo.getDomainCats().keySet())
				{				
					featlist.add(new DomCatFeature(onecat, true));
					featlist.add(new DomCatFeature(onecat, false));
				}			
			}		
			
			// Callout count
			{
				int[] callouts = new int[] { 0, 1, 2, 3, 4, 6, 8, 10, 15, 20, 25, 30, 35, 40, 50, 70, 100, 1000, 2000, 3000 };	
				
				for(int i = 0; i < callouts.length; i++)
				{
					int mi = callouts[i];
					int me = (i < callouts.length-1 ? callouts[i+1] : 100000000);
					featlist.add(new CalloutCountFeat(mi, me));
				}
			}			
			
			featlist.addAll(getBluekaiFeatSet());
			featlist.addAll(getExelateFeatSet());
			
			// IAB features
			for(Integer iabid : IABLookup.getSing().getNameMap().keySet())
			{
				IabStandardFeature isf = new IabStandardFeature(iabid);
				featlist.add(isf);
			}		
			
			SortedSet<UserFeature> featset = new TreeSet<UserFeature>(featlist);
			Util.massert(featset.size() == featlist.size(),
				"Feat list is size %d, feat set is size %d, duplicates detected in featlist");
			return featset;
		}
		
		static List<UserFeature> getExelateFeatSet()
		{
			List<UserFeature> exfeatlist = Util.vector();
			Map<Integer, String> exidmap = FeatureInfo.getTopExelateCategories();
			
			for(int exid : exidmap.keySet())
				{ exfeatlist.add(new ExelateFeature(exid)); }
			
			return exfeatlist;
		}
		
		static List<UserFeature> getBluekaiFeatSet()
		{
			List<UserFeature> bkfeatlist = Util.vector();
			Set<Integer> bksegs = BluekaiDataMan.getTaxonomy().getFeatIdSet();
			
			for(int bkid : bksegs)
				{ bkfeatlist.add(new BluekaiFeature(bkid)); }
			
			return bkfeatlist; 
		}
		
		private static void addSingleModePair(List<UserFeature> featlist, LogField fname, String targv)
		{
			featlist.add(new SingleMatch(fname, targv));	
			featlist.add(new ModeMatch(fname, targv));	
		}
		
		private static void addVarietyHitThresh(List<UserFeature> featlist, LogField fname, int hit)
		{
			featlist.add(new VarietyFeature(fname, hit, false));
			featlist.add(new VarietyFeature(fname, hit, true ));
		}	
	}
	
	private static class DomainAgg
	{
		private String _dayCode;
		
		private Map<CountryCode, SortedSet<Pair<Integer, String>>> _aggMap = Util.treemap();
		
		public DomainAgg(String dc)
		{
			TimeUtil.assertValidDayCode(dc);
			_dayCode = dc;
		}
		
		void loadDomainInfo()
		{
			double startup = Util.curtime();
			
			for(CountryCode cc : UserIndexUtil.COUNTRY_CODES)
			{
				Map<String, Integer> domhitmap = Util.treemap();
				
				// We want to go back a bit before, to make sure the days overlap with the scan block
				// Getting the exact right dates shouldn't matter so much 
				String gimp = TimeUtil.nDaysBefore(_dayCode, 7);
				
				for(int n = 0; n < 30; n++)
				{
					gimp = TimeUtil.dayBefore(gimp);
					String tdpath = UserIndexUtil.getTopDomainPath(cc, gimp);
					
					for(String oneline : FileUtils.readFileLinesE(tdpath))
					{
						// Util.pf("One line is %s\n", oneline);
						String[] toks = oneline.trim().split("\t");
						
						String domain = toks[0];
						if(!guessDomainOkay(domain))
							{ continue; }
						
						// TODO: DQ some domains if they don't look like real domains
						Util.setdefault(domhitmap, toks[0], 0);
						Util.incHitMap(domhitmap, toks[0], Integer.valueOf(toks[1]));
					}
				}
				
				SortedSet<Pair<Integer, String>> revset = new TreeSet<Pair<Integer, String>>(Collections.reverseOrder());
				
				for(Map.Entry<String, Integer> onehit : domhitmap.entrySet())
				{
					revset.add(Pair.build(onehit.getValue(), onehit.getKey()));	
				}
				
				_aggMap.put(cc, revset);
			}
			
			Util.pf("Done loading domain by country info, took %.03f secs, countries are %s\n",
				(Util.curtime()-startup)/1000, _aggMap.keySet());
		}
		
		
		// Heuristic guess about whether a string represents a domain or not 
		private boolean guessDomainOkay(String maybedomain)
		{
			String[] toks = maybedomain.split("\\.");
			return (toks.length == 2 || toks.length == 3);			
		}
		
		public SortedSet<Pair<Integer, String>> getTopDomSet(CountryCode ccode, int N)
		{
			Util.massert(_aggMap.containsKey(ccode),
				"Country code %s not found in agg map", ccode);
			
			TreeSet<Pair<Integer, String>> domset = new TreeSet<Pair<Integer, String>>(Collections.reverseOrder());
			
			domset.addAll(_aggMap.get(ccode));
			
			while(domset.size() > N)
				{ domset.pollLast(); }
			
			return domset;
		}
	}
	
	
	public static void main(String[] args) throws IOException
	{
		DomainAgg dagg = new DomainAgg(TimeUtil.getTodayCode());
		
		dagg.loadDomainInfo();
		
		SortedSet<Pair<Integer, String>> domset = dagg.getTopDomSet(CountryCode.US, 300);
		
		for(Pair<Integer, String> onepair : domset)
		{
			Util.pf("Domain %s got %d hits\n", onepair._2, onepair._1);
		}
		
	}
	
	public static Set<String> getBrowsers()
	{
		Set<String> bset = Util.treeset();
		
		bset.add("Chrome");
		bset.add("Firefox");
		bset.add("MSIE");
		bset.add("Opera");
		bset.add("Other");
		bset.add("Safari");		
		
		return bset;
	}	
	
	
	public static List<Integer> getVertList()
	{
		List<Integer> alist = Util.vector();
		alist.add(306);
		alist.add(335);
		alist.add(49);
		alist.add(5203);
		alist.add(5216);
		alist.add(5796);
		alist.add(705);
		alist.add(835);
		alist.add(256);
		alist.add(446);
		alist.add(5205);
		alist.add(5257);
		alist.add(5265);
		alist.add(5420);
		alist.add(5460);
		alist.add(628);
		alist.add(647);
		alist.add(791);
		alist.add(91);
		alist.add(97);
		alist.add(1007);
		alist.add(1146);
		alist.add(1166);
		alist.add(1176);
		alist.add(1263);
		alist.add(1291);
		alist.add(1296);
		alist.add(1312);
		alist.add(302);
		alist.add(5047);
		alist.add(5053);
		alist.add(611);
		alist.add(751);
		alist.add(800);
		alist.add(1004);
		alist.add(517);
		alist.add(672);
		alist.add(744);
		alist.add(832);
		alist.add(896);
		alist.add(1065);
		alist.add(1067);
		alist.add(1114);
		alist.add(1149);
		alist.add(1202);
		alist.add(1239);
		alist.add(1307);
		alist.add(1344);
		alist.add(248);
		alist.add(467);
		alist.add(468);
		alist.add(5789);
		alist.add(5798);
		alist.add(5799);
		alist.add(942);
		alist.add(5526);
		alist.add(635);
		alist.add(683);
		alist.add(1136);
		alist.add(1282);
		alist.add(1306);
		alist.add(5410);
		alist.add(5600);
		alist.add(658);
		alist.add(718);
		alist.add(861);
		alist.add(866);
		alist.add(915);
		alist.add(954);
		alist.add(1101);
		alist.add(1139);
		alist.add(1277);
		alist.add(1345);
		alist.add(168);
		alist.add(202);
		alist.add(5003);
		alist.add(5224);
		alist.add(5336);
		alist.add(5353);
		alist.add(551);
		alist.add(557);
		alist.add(5803);
		alist.add(621);
		alist.add(995);
		alist.add(1070);
		alist.add(1119);
		alist.add(1148);
		alist.add(1199);
		alist.add(1283);
		alist.add(546);
		alist.add(5801);
		alist.add(1137);
		alist.add(1284);
		alist.add(5413);
		alist.add(573);
		alist.add(731);
		alist.add(941);
		alist.add(1243);
		alist.add(1254);
		alist.add(1271);
		alist.add(1325);
		alist.add(1327);
		alist.add(488);
		alist.add(5329);
		alist.add(542);
		alist.add(5524);
		alist.add(5786);
		alist.add(5792);
		alist.add(210);
		alist.add(218);
		alist.add(441);
		alist.add(5172);
		alist.add(5282);
		alist.add(739);
		alist.add(808);
		alist.add(831);
		alist.add(903);
		alist.add(1249);
		alist.add(217);
		alist.add(309);
		alist.add(5208);
		alist.add(669);
		alist.add(1171);
		alist.add(1175);
		alist.add(1209);
		alist.add(1359);
		alist.add(297);
		alist.add(336);
		alist.add(342);
		alist.add(580);
		alist.add(996);
		alist.add(1203);
		alist.add(1225);
		alist.add(1318);
		alist.add(421);
		alist.add(427);
		alist.add(677);
		alist.add(727);
		alist.add(837);
		alist.add(1152);
		alist.add(1159);
		alist.add(120);
		alist.add(1237);
		alist.add(1269);
		alist.add(1292);
		alist.add(1323);
		alist.add(288);
		alist.add(380);
		alist.add(5211);
		alist.add(57);
		alist.add(5809);
		alist.add(665);
		alist.add(792);
		alist.add(955);
		alist.add(1015);
		alist.add(112);
		alist.add(1147);
		alist.add(1397);
		alist.add(445);
		alist.add(466);
		alist.add(5536);
		alist.add(5806);
		alist.add(645);
		alist.add(1233);
		alist.add(330);
		alist.add(5637);
		alist.add(1123);
		alist.add(1138);
		alist.add(124);
		alist.add(1280);
		alist.add(205);
		alist.add(495);
		alist.add(503);
		alist.add(5419);
		alist.add(962);
		alist.add(1161);
		alist.add(1227);
		alist.add(231);
		alist.add(450);
		alist.add(5261);
		alist.add(5522);
		alist.add(5797);
		alist.add(643);
		alist.add(850);
		alist.add(952);
		alist.add(1164);
		alist.add(1188);
		alist.add(632);
		alist.add(655);
		alist.add(812);
		alist.add(828);
		alist.add(859);
		alist.add(5221);
		alist.add(534);
		alist.add(547);
		alist.add(630);
		alist.add(821);
		alist.add(858);
		alist.add(946);
		alist.add(118);
		alist.add(1379);
		alist.add(5805);
		alist.add(917);
		alist.add(1120);
		alist.add(472);
		alist.add(500);
		alist.add(5632);
		alist.add(614);
		alist.add(426);
		alist.add(56);
		alist.add(625);
		alist.add(664);
		alist.add(959);
		alist.add(328);
		alist.add(461);
		alist.add(5207);
		alist.add(354);
		alist.add(963);
		alist.add(1217);
		alist.add(1257);
		alist.add(158);
		alist.add(296);
		alist.add(325);
		alist.add(5231);
		alist.add(5787);
		alist.add(639);
		alist.add(644);
		alist.add(750);
		alist.add(949);
		alist.add(1201);
		alist.add(244);
		alist.add(5238);
		alist.add(541);
		alist.add(648);
		alist.add(725);
		alist.add(818);
		alist.add(868);
		alist.add(965);
		alist.add(5220);
		alist.add(824);
		alist.add(1099);
		alist.add(1192);
		alist.add(1212);
		alist.add(1236);
		alist.add(290);
		alist.add(922);
		alist.add(991);
		alist.add(1350);
		alist.add(409);
		alist.add(5066);
		alist.add(521);
		alist.add(703);
		alist.add(953);
		alist.add(1075);
		alist.add(1088);
		alist.add(361);
		alist.add(636);
		alist.add(722);
		alist.add(856);
		alist.add(1061);
		alist.add(235);
		alist.add(280);
		alist.add(385);
		alist.add(508);
		alist.add(5234);
		alist.add(1016);
		alist.add(1190);
		alist.add(492);
		alist.add(734);
		alist.add(1150);
		alist.add(984);
		alist.add(1040);
		alist.add(527);
		alist.add(531);
		alist.add(651);
		alist.add(668);
		alist.add(748);
		alist.add(794);
		alist.add(801);
		alist.add(951);
		alist.add(976);
		alist.add(1198);
		alist.add(674);
		alist.add(992);
		alist.add(999);
		alist.add(333);
		alist.add(673);
		alist.add(969);
		alist.add(1235);
		alist.add(272);
		alist.add(1024);
		alist.add(1034);
		alist.add(331);
		alist.add(1169);
		alist.add(1221);
		alist.add(198);
		alist.add(253);
		alist.add(329);
		alist.add(386);
		alist.add(967);
		alist.add(1079);
		alist.add(1098);
		alist.add(1376);
		alist.add(362);
		alist.add(607);
		alist.add(883);
		alist.add(911);
		alist.add(552);
		alist.add(170);
		alist.add(237);
		alist.add(650);
		alist.add(706);
		alist.add(574);
		alist.add(1168);
		alist.add(1219);
		alist.add(1258);
		alist.add(144);
		alist.add(558);
		alist.add(1028);
		alist.add(940);
		alist.add(1022);
		alist.add(1117);
		alist.add(425);
		alist.add(5525);
		alist.add(634);
		alist.add(1111);
		alist.add(518);
		alist.add(5779);
		alist.add(1170);
		alist.add(633);
		alist.add(660);
		alist.add(1372);
		alist.add(257);
		alist.add(226);
		alist.add(42);
		alist.add(5228);
		alist.add(83);
		alist.add(157);
		alist.add(403);
		alist.add(451);
		alist.add(95);
		alist.add(379);
		alist.add(842);
		alist.add(916);
		alist.add(947);
		alist.add(1113);
		alist.add(1208);
		alist.add(689);
		alist.add(943);
		alist.add(1205);
		alist.add(255);
		alist.add(719);
		alist.add(804);
		alist.add(956);
		alist.add(471);
		alist.add(676);
		alist.add(84);
		alist.add(1246);
		alist.add(367);
		alist.add(610);
		alist.add(659);
		alist.add(803);
		alist.add(833);
		alist.add(195);
		alist.add(688);
		alist.add(1072);
		alist.add(1346);
		alist.add(1011);
		alist.add(1082);
		alist.add(1091);
		alist.add(5653);
		alist.add(667);
		alist.add(721);
		alist.add(749);
		alist.add(1288);
		alist.add(5232);
		alist.add(745);
		alist.add(1180);
		alist.add(171);
		alist.add(46);
		alist.add(1232);
		alist.add(54);
		alist.add(550);
		alist.add(681);
		alist.add(261);
		alist.add(559);
		alist.add(704);
		alist.add(964);
		alist.add(262);
		alist.add(640);
		alist.add(839);
		alist.add(613);
		alist.add(985);
		alist.add(1272);
		alist.add(312);
		alist.add(1009);
		alist.add(513);
		alist.add(723);
		alist.add(852);
		alist.add(904);
		alist.add(1178);
		alist.add(230);
		alist.add(807);
		alist.add(817);
		alist.add(882);
		alist.add(5409);
		alist.add(1141);
		alist.add(1216);
		alist.add(384);
		alist.add(671);
		alist.add(1019);
		alist.add(1073);
		alist.add(229);
		alist.add(250);
		alist.add(511);
		alist.add(53);
		alist.add(982);
		alist.add(1012);
		alist.add(1213);
		alist.add(443);
		alist.add(238);
		alist.add(5132);
		alist.add(809);
		alist.add(1000);
		alist.add(1010);
		alist.add(1133);
		alist.add(263);
		alist.add(5546);
		alist.add(802);
		alist.add(1211);
		alist.add(1248);
		alist.add(1260);
		alist.add(428);
		alist.add(869);
		alist.add(1215);
		alist.add(138);
		alist.add(5781);
		alist.add(77);
		alist.add(822);
		alist.add(836);
		alist.add(213);
		alist.add(1096);
		alist.add(1163);
		alist.add(189);
		alist.add(459);
		alist.add(487);
		alist.add(827);
		alist.add(918);
		alist.add(5591);
		alist.add(107);
		alist.add(1121);
		alist.add(246);
		alist.add(890);
		alist.add(910);
		alist.add(641);
		alist.add(863);
		alist.add(936);
		alist.add(657);
		alist.add(1006);
		alist.add(121);
		alist.add(5253);
		alist.add(5548);
		alist.add(1013);
		alist.add(5065);
		alist.add(82);
		alist.add(846);
		alist.add(887);
		alist.add(957);
		alist.add(993);
		alist.add(19);
		alist.add(536);
		alist.add(5627);
		alist.add(646);
		alist.add(1229);
		alist.add(1244);
		alist.add(884);
		alist.add(113);
		alist.add(369);
		alist.add(5249);
		alist.add(612);
		alist.add(912);
		alist.add(958);
		alist.add(305);
		alist.add(344);
		alist.add(1267);
		alist.add(5774);
		alist.add(900);
		alist.add(1087);
		alist.add(143);
		alist.add(145);
		alist.add(1108);
		alist.add(826);
		alist.add(1105);
		alist.add(1204);
		alist.add(1315);
		alist.add(166);
		alist.add(686);
		alist.add(11);
		alist.add(1262);
		alist.add(5807);
		alist.add(855);
		alist.add(950);
		alist.add(148);
		alist.add(576);
		alist.add(895);
		alist.add(5218);
		alist.add(1014);
		alist.add(346);
		alist.add(618);
		alist.add(1116);
		alist.add(457);
		alist.add(271);
		alist.add(510);
		alist.add(1081);
		alist.add(496);
		alist.add(64);
		alist.add(1102);
		alist.add(1177);
		alist.add(5212);
		alist.add(5516);
		alist.add(5783);
		alist.add(638);
		alist.add(913);
		alist.add(1046);
		alist.add(73);
		alist.add(48);
		alist.add(5077);
		alist.add(437);
		alist.add(685);
		alist.add(462);
		alist.add(242);
		alist.add(902);
		alist.add(502);
		alist.add(313);
		alist.add(1342);
		alist.add(701);
		alist.add(975);
		alist.add(998);
		alist.add(1103);
		alist.add(259);
		alist.add(78);
		alist.add(944);
		alist.add(5782);
		alist.add(5048);
		alist.add(811);
		alist.add(980);
		alist.add(989);
		alist.add(1026);
		alist.add(1127);
		alist.add(355);
		alist.add(70);
		alist.add(5633);
		alist.add(99);
		alist.add(424);
		alist.add(579);
		alist.add(96);
		alist.add(438);
		alist.add(474);
		alist.add(785);
		alist.add(1048);
		alist.add(323);
		alist.add(442);
		alist.add(575);
		alist.add(554);
		alist.add(276);
		alist.add(494);
		alist.add(5545);
		alist.add(743);
		alist.add(914);
		alist.add(694);
		alist.add(1266);
		alist.add(493);
		alist.add(717);
		alist.add(919);
		alist.add(287);
		alist.add(326);
		alist.add(1247);
		alist.add(5630);
		alist.add(301);
		alist.add(5226);
		alist.add(566);
		alist.add(733);
		alist.add(7);
		alist.add(311);
		alist.add(5014);
		alist.add(787);
		alist.add(1001);
		alist.add(843);
		alist.add(50);
		alist.add(1319);
		alist.add(228);
		alist.add(514);
		alist.add(788);
		alist.add(981);
		alist.add(1278);
		alist.add(838);
		alist.add(1140);
		alist.add(245);
		alist.add(1226);
		alist.add(893);
		alist.add(1050);
		alist.add(815);
		alist.add(691);
		alist.add(499);
		alist.add(5547);
		alist.add(568);
		alist.add(961);
		alist.add(741);
		alist.add(1112);
		alist.add(279);
		alist.add(58);
		alist.add(742);
		alist.add(675);
		alist.add(332);
		alist.add(1191);
		alist.add(977);
		alist.add(889);
		alist.add(509);
		alist.add(535);
		alist.add(892);
		alist.add(606);
		alist.add(1194);
		alist.add(278);
		alist.add(1276);
		alist.add(654);
		alist.add(1027);
		alist.add(522);
		alist.add(147);
		alist.add(307);
		alist.add(434);
		alist.add(5225);
		alist.add(5793);
		alist.add(485);
		alist.add(5523);
		alist.add(233);
		alist.add(1270);
		alist.add(813);
		alist.add(241);
		alist.add(269);
		alist.add(30);
		alist.add(586);
		alist.add(735);
		alist.add(1126);
		alist.add(1056);
		alist.add(533);
		alist.add(1085);
		alist.add(732);
		alist.add(292);
		alist.add(5416);
		alist.add(65);
		alist.add(987);
		alist.add(377);
		alist.add(1252);
		alist.add(375);
		alist.add(1055);
		alist.add(23);
		alist.add(420);
		alist.add(5631);
		alist.add(986);
		alist.add(555);
		alist.add(736);
		alist.add(1142);
		alist.add(374);
		alist.add(708);
		alist.add(983);
		alist.add(1008);
		alist.add(840);
		alist.add(929);
		alist.add(29);
		alist.add(5255);
		alist.add(662);
		alist.add(101);
		alist.add(360);
		alist.add(720);
		alist.add(310);
		alist.add(1187);
		alist.add(5227);
		alist.add(5562);
		alist.add(865);
		alist.add(894);
		alist.add(45);
		alist.add(1092);
		alist.add(188);
		alist.add(1095);
		alist.add(1025);
		alist.add(373);
		alist.add(1094);
		alist.add(447);
		alist.add(1131);
		alist.add(5254);
		alist.add(784);
		alist.add(338);
		alist.add(512);
		alist.add(608);
		alist.add(530);
		alist.add(350);
		alist.add(89);
		alist.add(1343);
		alist.add(37);
		alist.add(695);
		alist.add(224);
		alist.add(519);
		alist.add(5219);
		alist.add(1090);
		alist.add(752);
		alist.add(71);
		alist.add(678);
		alist.add(5086);
		alist.add(700);
		alist.add(104);
		alist.add(5204);
		alist.add(418);
		alist.add(814);
		alist.add(528);
		alist.add(906);
		alist.add(702);
		alist.add(615);
		alist.add(119);
		alist.add(270);
		alist.add(966);
		alist.add(653);
		alist.add(365);
		alist.add(538);
		alist.add(419);
		alist.add(1078);
		alist.add(5629);
		alist.add(400);
		alist.add(560);
		alist.add(1097);
		alist.add(216);
		alist.add(908);
		alist.add(572);
		alist.add(988);
		alist.add(1074);
		alist.add(806);
		alist.add(920);
		alist.add(1286);
		alist.add(16);
		alist.add(5171);
		alist.add(1089);
		alist.add(1032);
		alist.add(581);
		alist.add(891);
		alist.add(5092);
		alist.add(208);
		alist.add(1118);
		alist.add(314);
		alist.add(1023);
		alist.add(304);
		alist.add(289);
		alist.add(498);
		alist.add(1155);
		alist.add(1045);
		alist.add(885);
		alist.add(1122);
		alist.add(293);
		alist.add(366);
		alist.add(477);
		alist.add(1230);
		alist.add(5777);
		alist.add(227);
		alist.add(899);
		alist.add(258);
		alist.add(1107);
		alist.add(44);
		alist.add(582);
		alist.add(347);
		alist.add(174);
		alist.add(1076);
		alist.add(1093);
		alist.add(98);
		alist.add(225);
		alist.add(31);
		alist.add(18);
		alist.add(264);
		alist.add(1167);
		alist.add(486);
		alist.add(909);
		alist.add(303);
		alist.add(1109);
		alist.add(1041);
		alist.add(539);
		alist.add(1031);
		alist.add(567);
		alist.add(1033);
		alist.add(1044);
		alist.add(5210);
		alist.add(315);
		alist.add(458);
		alist.add(1043);
		alist.add(456);
		alist.add(501);
		alist.add(1174);
		alist.add(39);
		alist.add(1106);
		alist.add(505);
		alist.add(1039);
		alist.add(203);
		alist.add(93);
		alist.add(343);
		alist.add(13);
		alist.add(699);
		alist.add(516);
		alist.add(563);
		alist.add(1035);
		alist.add(180);
		alist.add(94);
		alist.add(1084);
		alist.add(933);
		alist.add(1100);
		alist.add(997);
		alist.add(236);
		alist.add(848);
		alist.add(76);
		alist.add(1047);
		alist.add(587);
		alist.add(609);
		alist.add(948);
		alist.add(693);
		alist.add(63);
		alist.add(12);
		alist.add(5235);
		alist.add(592);
		alist.add(805);
		alist.add(398);
		alist.add(870);
		alist.add(728);
		alist.add(520);
		alist.add(1193);
		alist.add(616);
		alist.add(444);
		alist.add(47);
		alist.add(59);
		alist.add(1231);
		alist.add(888);
		alist.add(1287);
		alist.add(1104);
		alist.add(401);
		alist.add(66);
		alist.add(396);
		alist.add(921);
		alist.add(497);
		alist.add(32);
		alist.add(440);
		alist.add(185);
		alist.add(435);
		alist.add(284);
		alist.add(43);
		alist.add(254);
		alist.add(25);
		alist.add(137);
		alist.add(378);
		alist.add(5230);
		alist.add(433);
		alist.add(1290);
		alist.add(697);
		alist.add(5054);
		alist.add(1132);
		alist.add(449);
		alist.add(569);
		alist.add(20);
		alist.add(432);
		alist.add(1265);
		alist.add(60);
		alist.add(36);
		alist.add(925);
		alist.add(234);
		alist.add(928);
		alist.add(1223);
		alist.add(61);
		alist.add(901);
		alist.add(268);
		alist.add(67);
		alist.add(74);
		alist.add(1110);
		alist.add(115);
		alist.add(14);
		alist.add(939);
		alist.add(324);
		alist.add(1049);
		alist.add(515);
		alist.add(68);
		alist.add(239);
		alist.add(594);
		alist.add(5);
		alist.add(179);
		alist.add(532);
		alist.add(979);
		alist.add(439);
		alist.add(923);
		alist.add(1285);
		alist.add(585);
		alist.add(1134);
		alist.add(1080);
		alist.add(5206);
		alist.add(1183);
		alist.add(371);
		alist.add(525);
		alist.add(1038);
		alist.add(33);
		alist.add(1077);
		alist.add(972);
		alist.add(215);
		alist.add(436);
		alist.add(408);
		alist.add(543);
		alist.add(5024);
		alist.add(990);
		alist.add(690);
		alist.add(412);
		alist.add(937);
		alist.add(1242);
		alist.add(1037);
		alist.add(299);
		alist.add(1222);
		alist.add(907);
		alist.add(473);
		alist.add(75);
		alist.add(146);
		alist.add(390);
		alist.add(273);
		alist.add(692);
		alist.add(34);
		alist.add(372);
		alist.add(1036);
		alist.add(588);
		alist.add(504);
		alist.add(422);
		alist.add(316);
		alist.add(656);
		alist.add(507);
		alist.add(578);
		alist.add(540);
		alist.add(565);
		alist.add(320);
		alist.add(8);
		alist.add(1195);
		alist.add(321);
		alist.add(932);
		alist.add(886);
		alist.add(381);
		alist.add(318);
		alist.add(548);
		alist.add(102);
		alist.add(590);
		alist.add(359);
		alist.add(220);
		alist.add(864);
		alist.add(122);
		alist.add(1311);
		alist.add(1021);
		alist.add(108);
		alist.add(252);
		alist.add(591);
		alist.add(577);
		alist.add(935);
		alist.add(622);
		alist.add(448);
		alist.add(593);
		alist.add(927);
		alist.add(5217);
		alist.add(24);
		alist.add(740);
		alist.add(847);
		alist.add(930);
		alist.add(358);
		alist.add(960);
		alist.add(275);
		alist.add(191);
		alist.add(1264);
		alist.add(737);
		alist.add(931);
		alist.add(5001);
		alist.add(319);
		alist.add(1030);
		alist.add(184);
		alist.add(1184);
		alist.add(382);
		alist.add(926);
		alist.add(55);
		alist.add(978);
		alist.add(1173);
		alist.add(211);
		alist.add(1135);
		alist.add(105);
		alist.add(357);
		alist.add(3);
		alist.add(317);
		alist.add(41);
		alist.add(35);
		alist.add(1071);
		alist.add(182);
		alist.add(617);
		alist.add(394);
		alist.add(100);
		alist.add(294);
		alist.add(529);
		alist.add(1279);
		alist.add(22);
		return alist;
	}	
}
