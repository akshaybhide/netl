
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.LogType;
import com.adnetik.userindex.UserIndexUtil.*;

// Handles the work of generating a feature report and learning a classifier.
// TODO: rename this to something more intelligeble.
// This is actually going to become two classes, FeatureReport and 
// something else
public class UIndexDataManager
{		
	private Map<String, boolean[]> boolMap = Util.treemap();
	
	private boolean[] targVec;
	
	
	public UIndexDataManager(PrecompFeatPack poscalc, PrecompFeatPack negcalc, boolean removepos, boolean removeneg)
	{
		/*
		Util.pf("\nCombining feature results...");
		targVec = PrecompFeatPack.genTargVec(poscalc, negcalc);
		boolMap = PrecompFeatPack.combine(poscalc, negcalc, removepos, removeneg);
		Util.pf(" ... done");	
		*/
	}
	
	public boolean[] getTargVec()
	{
		return targVec;	
	}
	
	public Map<String, boolean[]> getFullMap()
	{
		return Collections.unmodifiableMap(boolMap);
	}
		
	
	public Map<String, boolean[]> getSubFeatureMap(Set<FeatureCode> codeset)
	{
		Map<String, boolean[]> subfeatmap = Util.treemap();

		for(String funcname : boolMap.keySet())
		{
			Util.massert(StrayerFeat.getFeatMap().containsKey(funcname), "Function name %s not found in FeatureMap");
			
			FeatureCode fcode = StrayerFeat.getFeatMap().get(funcname).getCode();
			
			if(codeset.contains(fcode))
				{ subfeatmap.put(funcname, boolMap.get(funcname)); }
		}
		
		Util.pf("Full map has %d features, subfeatmap has %d features \nFor codeset=%s", 
			boolMap.size(), subfeatmap.size(), codeset);

		return subfeatmap;		
	}
	
	/*
	public Map<String, boolean[]> getBasicMap()
	{
		Map<String, boolean[]> basicmap = Util.treemap();

		for(String funcname : boolMap.keySet())
		{
			Util.massert(StrayerFeat.getFeatMap().containsKey(funcname), "Function name %s not found in FeatureMap");
			
			if(!StrayerFeat.getFeatMap().get(funcname).getCode().isThirdParty())
				{ basicmap.put(funcname, boolMap.get(funcname)); }
		}
		
		Util.pf("Full map has %d features, basic map has %d features\n", boolMap.size(), basicmap.size());
		
		return basicmap;
	}	
	*/
	
	public Map<String, boolean[]> getOneFeatureCodeMap(FeatureCode toget)
	{
		Map<String, boolean[]> codemap = Util.treemap();

		for(String funcname : boolMap.keySet())
		{
			if(StrayerFeat.getFeatMap().get(funcname).getCode() == toget)
				{ codemap.put(funcname, boolMap.get(funcname)); }
		}
		
		Util.pf("Full map has %d features, basic map has %d features\n", boolMap.size(), codemap.size());
		
		return codemap;
	}		
	
	public Map<String, boolean[]> getBasicPlusFeature(FeatureCode toget)
	{
		throw new RuntimeException("Not yet implemented");
		/*
		Map<String, boolean[]> bmap = getBasicMap();
		bmap.putAll(getOneFeatureCodeMap(toget));
		return bmap;
		*/
	}		
}
