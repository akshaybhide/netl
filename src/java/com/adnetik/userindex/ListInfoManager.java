
package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.sql.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.shared.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

import com.adnetik.data_management.ParamPixelMan;

// Manage pixel lists, etc
public class ListInfoManager
{	
	private enum CLarg { showlistsize, showdateinfo, showmulti, pxprmtest, addnew, cancel, showall, showdefsize, setlistsize, showdetail, showactive, showregion };
	
	private static ListInfoManager _SING = null;
	
	private SortedMap<String, PosRequest> _posReqMap;
	private SortedMap<String, NegRequest> _negReqMap;
	
	// Country code --> Default List size
	SortedMap<CountryCode, Integer> _defListSize = Util.treemap();

	private static Set<String> _US_DATA_CENTER = Util.treeset();
	private static Set<String> _EU_DATA_CENTER = Util.treeset();
	
	static {
		_US_DATA_CENTER.addAll(Arrays.asList(new String[] { "US", "BR", "MX" }));
		_EU_DATA_CENTER.addAll(Arrays.asList(new String[] { "NL", "GB", "ES" }));
	}
	
	public static void main(String[] args) throws Exception
	{
		CLarg theop;
		try { theop = CLarg.valueOf(args[0]); }
		catch (Exception ex) {
			Util.pf("Usage: ListInfoManager <addnew|cancel|showall>\n");	
			return;
		}
		
		if(theop == CLarg.showall)
		{
			showAll();
		}
		
		if(theop == CLarg.showactive)
		{
			showActive();
		}		
		
		if(theop == CLarg.showdetail)
		{
			showDetails();
		}		

		if(theop == CLarg.showdateinfo)
		{
			showDateInfo();
		}				
		
		if(theop == CLarg.setlistsize)
		{
			setListSize();
		}		
		
		if(theop == CLarg.showmulti)
		{
			getSing();
			
			for(PosRequest preq  : getSing().getPosRequestSet())
			{
				// Util.pf("Listcode %s is type %s\n", preq.getListCode(), preq.getClass().getName());
				
				if(!(preq instanceof MultiPixRequest))
					{ continue; }
				
				MultiPixRequest mpr = (MultiPixRequest) preq;
				
				Util.pf("MPR %s :: %s has pixel set %s\n",
					mpr.getListCode(), mpr.getOldListCode(), mpr.getPixSet());
			}
		}
		
		if(theop == CLarg.pxprmtest)
		{
			ListInfoManager sing = getSing();
			sing.loadRequestMaps();
			
			PxprmRequest pxreq = (PxprmRequest) sing.getPosRequest("pxprm_0014965_A");
			
			Set<WtpId> idset = pxreq.grabIdSet();
			
			Util.pf("Found %d PXPRM IDs\n", idset.size());
			
		}
		
		if(theop == CLarg.showdefsize)
		{
			Map<CountryCode, Integer> defmap = getSing()._defListSize;	
			
			for(Map.Entry<CountryCode, Integer> defentry : defmap.entrySet())
			{
				Util.pf("FOund %d K users for country %s\n", 
					defentry.getValue(), defentry.getKey());	
			}
		}
		
		if(theop == CLarg.showlistsize)
		{
			for(PosRequest preq : getSing().getPosRequestSet())
			{
				Util.pf("PosReq %s is size %d\n",
					preq.getListCode(), preq.getTargSizeK());
			}
		}
		
		if(theop == CLarg.cancel)
		{
			throw new RuntimeException("Not yet implemented");
			
		}
	}
	
	public synchronized int getDefaultListSizeK(String countrycode)
	{
		Util.massert(_defListSize.containsKey(countrycode), 
			"Country code %s not found in default list size map", countrycode);
		
		return _defListSize.get(countrycode);
	}
		
	public boolean havePosRequest(String listcode)
	{
		return _posReqMap.containsKey(listcode);	
	}
	
	public PosRequest getPosRequest(String listcode)
	{
		return _posReqMap.get(listcode);	
	}
	
	public boolean haveRequest(String listcode)
	{
		return _posReqMap.containsKey(listcode) || _negReqMap.containsKey(listcode);
	}
	
	public ScanRequest getRequest(String listcode)
	{
		return (listcode.startsWith(StagingType.negative.toString()) ? _negReqMap : _posReqMap).get(listcode);
	}
	
	public String getNegListCode(String poscode)
	{
		Map<String, String> p2n = getPos2NegMap();
		Util.massert(p2n.containsKey(poscode), "Could not find positive code : " +poscode);
		return p2n.get(poscode);
	}
	
	
	public Map<String, String> getPos2NegMap()
	{
		Map<String, String> p2nmap = Util.treemap();
		
		for(PosRequest preq : _posReqMap.values())
		{
			p2nmap.put(preq.getListCode(), preq.getNegRequest().getListCode());
		}
		
		return p2nmap;
	}
	
	// Can be null!!
	public Integer getAdbListId(String poscode)
	{
		Util.massert(havePosRequest(poscode), "No request found for listcode %s", poscode);
		
		return getPosRequest(poscode).getAdbListId();
	}
	
	public String getExtListId(String poscode)
	{
		Util.massert(havePosRequest(poscode), "No request found for listcode %s", poscode);
		
		return getPosRequest(poscode).getExtListId();		
	}
		
	public boolean hasGeoSkew(String listcode)
	{
		Util.massert(haveRequest(listcode), "No request found for listcode %s", listcode);
		
		return getRequest(listcode).hasGeoSkew();
	}
	
	public CountryCode getCountryTargForList(String listcode)
	{
		Util.massert(haveRequest(listcode), "No request found for listcode %s", listcode);
		
		return getRequest(listcode).getCountryCode();
	}
	
	public String getNickName(String listcode)
	{
		Util.massert(haveRequest(listcode), "No request found for listcode %s", listcode);
		
		return getRequest(listcode).getNickName();
	}
	
	public String getExpireDate(String listcode)
	{
		Util.massert(haveRequest(listcode), "No request found for listcode %s", listcode);
		
		return getRequest(listcode).getExpirationDate();
	}
	
	public boolean isActive(String listcode)
	{
		return isActiveOn(listcode, TimeUtil.getTodayCode());
		
	}
	
	public boolean isActiveOn(String listcode, String daycode)
	{
		Util.massert(haveRequest(listcode), "No entry found for listcode %s", listcode);
		
		return getRequest(listcode).isActiveOn(daycode);
	}	
	
	// TODO: remove this in favor of a map that is loaded
	// directly from the database, allowing for dynamic expansion
	// of country target list	
	public String getDataCenterForList(String listcode)
	{
		String ctry = getCountryTargForList(listcode).toString();
		
		synchronized(_US_DATA_CENTER) {
			if(_US_DATA_CENTER.contains(ctry))
				{ return "US"; }
		}
		
		synchronized(_EU_DATA_CENTER) {
			if(_EU_DATA_CENTER.contains(ctry))
				{ return "EU"; }
		}	
	
		throw new RuntimeException("No datacenter for country " + ctry);
	}
	


	public static synchronized ListInfoManager getSing()
	{
		if(_SING == null)
			{ rebuildSing(); }
		
		return _SING;
	}
	
	// Rebuild the singleton from the DB. 
	// This is important for long-running jobs, because otherwise
	// their version of the LIM will get out of date.
	static synchronized void rebuildSing()
	{
		Util.pf("Rebuilding ListInfoManager Singleton... ");
		_SING = new ListInfoManager();
		_SING.loadUtilMaps();
		_SING.loadRequestMaps();
		Util.pf(" ... done\n");		
	}
	
	private synchronized void loadUtilMaps()
	{
		String sql = "SELECT country, size_in_k FROM default_list_size";
		
		Map<String, Integer> defmap = Util.treemap();
		
		DbUtil.popMapFromQuery(defmap, sql, new UserIdxDb());
		
		for(String cc : defmap.keySet())
		{ 
			_defListSize.put(CountryCode.valueOf(cc), defmap.get(cc));	
		}
		
		// Util.pf("Loaded list size map : %s\n", _defListSize);
	}
	
	private synchronized void loadRequestMaps()
	{
		Util.massert(_defListSize != null && !_defListSize.isEmpty(),
			"Must initialized Utility map data before request codes");
		
		Util.massert(_posReqMap == null && _negReqMap == null,
			"Request maps are already loaded");
		
		_posReqMap = Util.treemap();
		_negReqMap = Util.treemap();
		
		// Pull the positive requests from the DB
		{		
			List<ScanRequest> poslist = ScanRequest.grabFromDB(new UserIdxDb());			
			for(ScanRequest onescan : poslist)
			{
				Util.putNoDup(_posReqMap, onescan.getListCode(), (PosRequest) onescan);
				
				if(onescan instanceof SpecpccRequest)
				{
					SpecpccRequest specreq = (SpecpccRequest) onescan;
					Util.putNoDup(_posReqMap, specreq.getOldCode(), specreq);
				}
				
				if(onescan instanceof PosRequest)
				{
					PosRequest preq = (PosRequest) onescan;
					if(preq.getTargSizeK() == null)
					{
						preq.setTargSizeK(_defListSize.get(onescan.getCountryCode()));
					}
				}
			}	
		}
		
		// Create the negative requests
		for(NegRequest nreq : ScanRequest.getNegReqMap().values())
		{
			Util.putNoDup(_negReqMap, nreq.getListCode(), nreq);
		}
	}
	
	public Set<PosRequest> getPosRequestSet()
	{
		// Ugh
		Set<PosRequest> pset = Util.treeset();
		pset.addAll(_posReqMap.values());
		return pset;
	}

	public SortedSet<String> getFullListCodeSet()
	{
		SortedSet<String> listset = Util.treeset();
		Map<String,String> pos2neg = getPos2NegMap();
		listset.addAll(pos2neg.keySet());
		listset.addAll(pos2neg.values());
		return listset;
	}
	
	
	public SortedSet<ScanRequest> getFullScanReqSet()
	{
		SortedSet<ScanRequest> reqset = Util.treeset();
		reqset.addAll(_posReqMap.values());
		reqset.addAll(_negReqMap.values());
		return reqset;		
	}
	
	// Change this out for getListenCodesByType(StagingType stype)
	public Set<String> getListenCodesByPref(String pref)
	{
		Set<String> prefset = Util.treeset();
		
		for(String onecode : getFullListCodeSet())
		{
			if(onecode.startsWith(pref))
				{ prefset.add(onecode); }
		}
		
		return prefset;
	}		
	
	public static void showAll()
	{	
		//Util.pf("List info is %s\n", lman._ctryMap);
		//Util.pf("Nick info is %s\n", lman._nickMap);
		
		for(PosRequest preq : getSing()._posReqMap.values())
		{
			Util.pf("Going to score %d thousand users for client=%s, listcode %s, expiration is %s\n", 
				preq.getTargSizeK(), preq.getNickName(), preq.getListCode(), preq.getExpirationDate());
		}
	}
	
	public static void showDateInfo()
	{
		for(ScanRequest screq : getSing().getFullScanReqSet())
		{
			Util.pf("Scan Request %s  :: entry=%s :: expire=%s :: active=%s\n",
				Util.padstr(screq.getListCode(), 25), screq.getEntryDate(), screq.getExpirationDate(), screq.isActive());
		}		
	}
	
	public static void showActive()
	{	
		Util.pf("ACTIVE LIST REQUESTS\n");
			
		for(PosRequest preq : getSing()._posReqMap.values())
		{
			if(preq.isActive())
			{
				Util.pf("\tListcode %s, nickname %s, expdate %s, requester %s\n", 
					preq.getListCode(), preq.getNickName(), 
					preq.getExpirationDate(), preq.getRequester());
			}
			
		}
	}	

	// Adds the pos request to the batch of lists under management,
	// and inserts it into the underlying database.
	void insertListCodeShell(PosRequest preq)
	{
		Util.putNoDup(_posReqMap, preq.getListCode(), preq);
		
		// Need the shell before you use persist()
		UserIdxDb.insertListCodeShell(preq.getListCode(), preq.getCountryCode().toString(), preq.getEntryDate());
	}
	
	public static void showDetails() throws Exception
	{
		ListInfoManager lman = ListInfoManager.getSing();
		
		/*
		String listcode = Util.promptUser("Enter a list code: ");
		if(!lman._ctryMap.containsKey(listcode))
		{
			Util.pf("List code not found %s\n", listcode);	
			return;
		}
		
		Util.pf("listcode  \t%s \ncountry   \t%s\ntargsize  \t%d \ngeoskew   \t%b\n", 
			listcode, lman._ctryMap.get(listcode), lman._listSizeMap.get(listcode), lman._geoSkewMap.get(listcode) > 0);
		
		Util.pf("entrydate \t%s\n", lman.getPosRequest(listcode).getEntryDate());
		*/
		
	}	
	
	// Set of feature codes that are valid for being used to learn for a listcode.
	public Set<FeatureCode> getLearnCodes4List(PosRequest posreq)
	{
		boolean geoskew = posreq.hasGeoSkew();
		
		Set<FeatureCode> fset = Util.treeset();
		for(FeatureCode fcode : FeatureCode.values())
		{
			// TODO: figure out how/when to add 3party feature codes.
			if(fcode.isThirdParty())
				{ continue; }
			
			if(!fcode.isRegionalCode() || !geoskew)
				{ fset.add(fcode); }
		}
		
		return fset;
	}
	
	// TODO: this should really be deprecated
	// This is really just a terrible method for doing this stuff
	public Map<String, Integer> getListenCodeMap()
	{
		Set<String> codeset = getFullListCodeSet();
		
		Map<String, Integer> codemap = Util.treemap();
		
		// This needs to be the same behavior as the SmartPartitioner
		codemap.put(SpecialCode.NOTFOUND.toString(), 0);
		
		for(String onecode : codeset)
		{ 
			codemap.put(onecode, codemap.size()); 
		}	
	
		return codemap;
	}		
	
	public static void setListSize() throws Exception
	{
		ListInfoManager lman = ListInfoManager.getSing();
		
		String listcode = Util.promptUser("Enter a list code: ");
		if(!lman.havePosRequest(listcode))
		{
			Util.pf("List code not found %s\n", listcode);	
			return;
		}

		String listsize = Util.promptUser("Enter the target list size in millions: ");
		Integer newtarg = Integer.valueOf(listsize);

		String checkprompt = Util.sprintf("Okay to update size target for listcode %s  from %d --> %d million",
							listcode, lman.getPosRequest(listcode).getTargSizeK(), newtarg);
		if(Util.checkOkay(checkprompt))
		{
			String sql = Util.sprintf("UPDATE listen_code SET targsize_mil = %d where listcode = '%s'", newtarg, listcode);
			DbUtil.execSqlUpdate(sql, new UserIdxDb());
		}
	}	
}
