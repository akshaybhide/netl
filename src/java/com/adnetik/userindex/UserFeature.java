
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.UserIndexUtil.*;

import com.adnetik.data_management.BluekaiDataMan;
import com.adnetik.data_management.ExelateDataMan;


public abstract class UserFeature extends BinaryFeature<UserPack> implements Comparable<UserFeature>
{
	public abstract FeatureCode getCode();
	
	// public abstract int compareToSub(UserFeature sameclass)		
	
	private static HashMap<String, UserFeature> _NAME_KEY_MAP = Util.hashmap();
	
	public static synchronized UserFeature buildFromNameKey(String logrec)
	{
		String lrs = logrec.trim();
		
		if(!_NAME_KEY_MAP.containsKey(lrs))
		{
			String[] toks = logrec.split("\t");
			String[] keylist = new String[toks.length-1];
			
			for(int i = 1; i < toks.length; i++)
				{ keylist[i-1] = toks[i]; }
			
			try {
				String canonname = toks[0];
				UserFeature onefeat = (UserFeature) Class.forName(canonname).newInstance();
				onefeat.buildFromKeyList(keylist);
				_NAME_KEY_MAP.put(lrs, onefeat);

				Util.massert(_NAME_KEY_MAP.size() < 1000000,
					"NameKey map is too big");
				
			} catch (Exception ex) {
				Util.pf("Error building UserFeature for logrec=%s\n", logrec);
				throw new RuntimeException(ex);	
			}			
		}
		
		return _NAME_KEY_MAP.get(lrs);
	}
	
	public static Map<String, UserFeature> buildFeatMap(Collection<String> namekeylist)
	{
		Map<String, UserFeature> featmap = Util.treemap();
		
		for(String namekey : namekeylist)
		{
			featmap.put(namekey, buildFromNameKey(namekey));
		}
		
		return featmap;
	}
	
	public int compareTo(UserFeature that)
	{
		return getNameKey().compareTo(that.getNameKey());
	}
	
	String getNameKey()
	{
		// return Util.varjoin("\t", getCode(), getKeyList());	
		return getClass().getName() + "\t" + Util.join(getKeyList(), "\t");
	}
	
	String getKeyListStr()
	{
		return Util.join(getKeyList(), "\t");
	}
	
	abstract Object[] getKeyList();
	
	abstract void buildFromKeyList(String[] keylist); 
	
	@Override
	public int hashCode()
	{
		Util.massert(false, "Do not call this method/do not attempt to put UserFeatures into hashmaps, must use treemaps");	
		return -1;
	}		
	
	// This is the feature's display name/category that gets shown to the 
	// user in reporting. By default, it is the same as the feature code,
	// but in some cases we want to override.
	public String getDisplayCat()
	{
		return getCode().toString();
	}

	// Lookup the given UserFeature in the registry.
	// This method is called by the lookup(...) methods on each specific Feature object.
	// So instead of a public constructor, you call lookup, which calls the private constructor,
	// then calls lookupFromReg(..) with the result.
	/*
	private static UserFeature lookupFromReg(UserFeature gimp)
	{
		if(!_REG_SET.contains(gimp))
		{
			Util.massert(!_SEALED, "Feature %s is not in registry and it has been sealed", gimp);
			
			// Here we are okay to put the gimp in the registry
			Util.putNoDup(_REG_SET, gimp);
			return gimp;				
		}
		
		UserFeature singfeat = _REG_SET.floor(gimp);
		
		Util.massert(singfeat != null && singfeat.compareTo(gimp) == 0,
			"Error looking up feature %s, thought it was %s",
			singfeat, gimp);
		
		return singfeat;
	}
	*/
	
	public static class NullFeat extends UserFeature
	{
		NullFeat() {} 
		
		public boolean evalSub(UserPack up)
		{
			return true;
		}
		
		public String toString()
		{
			return Util.sprintf("NullFeat");	
		}		
		
		public FeatureCode getCode()
		{
			return FeatureCode.noop;
		} 
		
		/*
		public static NullFeat lookup()
		{
			return (NullFeat) lookupFromReg(new NullFeat());	
		}
		*/
		
		public Object[] getKeyList()
		{
			return new Object[] {} ;	
		}
		
		public void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 0);	
		}
	}
	
	public interface HasParty3Info
	{
		public Pair<Part3Code, Integer> getSegmentInfo();
	}
	
	public static class SimplePixelFeature extends UserFeature
	{
		Integer _pixTarg;
		
		SimplePixelFeature()
		{
			
		}
		
		SimplePixelFeature(int ptarg)
		{
			_pixTarg = ptarg;
		}
		
		public boolean evalSub(UserPack up)
		{
			// Util.pf("Calling EvalSub with pixTarg=%d, num pixel entries is %d\n", _pixTarg, up.getPixList().size());
			
			for(PixelLogEntry ple : up.getPixList())
			{
				// Classic bug: was using == here, instead of equals(..); 
				// the arithmetic equals works if you are using primitive integers but not
				// object Integers
				if(ple.getIntField(LogField.pixel_id).equals( _pixTarg))
					{ return true; }
			}
			
			return false;
		}
		
		public String toString()
		{
			// TODO: look up pixel names
			return Util.sprintf("SimplePixel ID=%d", _pixTarg);	
		}		
		
		public FeatureCode getCode()
		{
			return FeatureCode.pixel;
		} 		
		
		public Object[] getKeyList()
		{
			return new Object[] { _pixTarg } ;	
		}
		
		public void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 1);
			_pixTarg = Integer.valueOf(keylist[0]);
		}		
		
	}
	
	// TODO: can we combine the ExelateFeature and BluekaiFeature?
	public static class BluekaiFeature extends UserFeature
		implements BluekaiFeatureFunc, HasParty3Info
	{
		Integer _segId; 
		
		BluekaiFeature()
		{
			
		}
		
		BluekaiFeature(int sid)
		{
			_segId = sid;
		}
		
		public boolean bkEval(BluekaiDataMan.BluserPack bup)
		{
			Util.massert(bup != null, "Do not call with null BluserPack");
			return bup.hasSegmentEver(_segId); 
		}
		
		public boolean evalSub(UserPack up)
		{
			try {
				BluekaiDataMan.BluserPack bup = up.getBluePack();
				return bup != null && bkEval(bup);
				
			} catch (IOException ioex) {
				
				throw new RuntimeException(ioex);	
			}
		}
		
		public String toString()
		{
			String segname = BluekaiDataMan.getTaxonomy().getFeatName(_segId);
			return Util.sprintf("BlueKai : %s", segname);
		}			
		
		public FeatureCode getCode() 
		{
			return FeatureCode.bluekai;	
		}
		
		public Pair<Part3Code, Integer> getSegmentInfo()
		{
			return Pair.build(Part3Code.BK, _segId);
		}
		
		public Object[] getKeyList()
		{
			return 	new Object[] { _segId };
		}
		
		public void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 1);
			_segId = Integer.valueOf(keylist[0]);
		}			
	}

	public static class ExelateFeature extends UserFeature
		implements ExelateFeatureFunc, HasParty3Info
	{
		Integer _segId;
		
		ExelateFeature()
		{
			
		}
		
		ExelateFeature(int exid)
		{
			_segId = exid;
		}
		
		public boolean exEval(ExelateDataMan.ExUserPack expack)
		{
			Util.massert(expack != null, "Do not call with null ExUserPack");
			return expack.hasSegmentEver(_segId); 			
		}
		
		public boolean evalSub(UserPack up)
		{
			try {
				ExelateDataMan.ExUserPack exup = up.getExelatePack();
				return exup != null && exEval(exup);
				
			} catch (IOException ioex) {
				
				throw new RuntimeException(ioex);	
			}
		}
		
		public String toString()
		{
			return Util.sprintf("ExelateFeature for Segment %s (%d)", 
				FeatureInfo.getTopExelateCategories().get(_segId), _segId);	
		}			
		
		public FeatureCode getCode() 
		{
			return FeatureCode.exelate;	
		}
		
		public Pair<Part3Code, Integer> getSegmentInfo()
		{
			return Pair.build(Part3Code.EX, _segId);
		}	
		
		public Object[] getKeyList()
		{
			return new Object[] { _segId };
		}
		
		public void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 1);
			_segId = Integer.valueOf(keylist[0]);
		}			
	}	
	
	public static class HispanicFeature extends UserFeature
	{
		double _minFrac;
		double _maxFrac;
		
		Integer _quintile;
		
		HispanicFeature()
		{
			
		}
		
		public HispanicFeature(int q)
		{
			_quintile = q;
			initFromQuintile();
		}
		
		private void initFromQuintile()
		{
			_minFrac = _quintile+0;
			_maxFrac = _quintile+1;
			
			_minFrac /= 5;
			_maxFrac /= 5;			
		}
		
		public boolean evalSub(UserPack up)
		{
			String zip = up.getFieldMode(LogField.user_postal);
			
			Double hfrac = FeatureInfo.hispanicDensityByZip().get(zip);
			
			// Util.pf("User postal is %s, fraction is %.03f\n", zip, hfrac);
			
			if(hfrac == null)
				{ return false; }
			
			return _minFrac <= hfrac && hfrac <= _maxFrac;
		}
		
		public String toString()
		{
			return Util.sprintf("User Zip Hispanic Density between %.02f and %.02f", _minFrac, _maxFrac); 
		}			
		
		public FeatureCode getCode()
		{
			return FeatureCode.demographic;
		}
		
		public Object[] getKeyList()
		{
			return new Object[] { _quintile } ;
		}
		
		public void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 1);
			_quintile = Integer.valueOf(keylist[0]);
			initFromQuintile();
		}			
	}	
	
	public static class IabStandardFeature extends UserFeature
	{
		private Integer _targIabId;
		
		IabStandardFeature()
		{
			
		}
		
		IabStandardFeature(int iabid)
		{
			_targIabId = iabid;
		}
		
		public boolean evalSub(UserPack up)
		{
			for(BidLogEntry ble : up.getBidList())
			{
				try { 
					Set<Integer> iabsegset = ble.getIabSegSet(); 
					
					if(iabsegset.contains(_targIabId))
						{ return true; }					
					
				} catch (BidLogFormatException blex) { }
			}	
			
			return false;
		}
		
		public String toString()
		{
			return Util.sprintf("OneOrMoreOf: IAB Segment %s (%d)", 
				IABLookup.getSing().getNameMap().get(_targIabId), _targIabId);
		}			
		
		public FeatureCode getCode()
		{
			return FeatureCode.iab;
		}
		
		public Object[] getKeyList()
		{
			return new Object[] { _targIabId };
		}
		
		void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 1);
			_targIabId = Integer.valueOf(keylist[0]);
		}			
		
	}

	public static class CalloutCountFeat extends UserFeature
	{
		Integer minInc;
		Integer maxExc;
		
		CalloutCountFeat()
		{
			
		}
		
		CalloutCountFeat(int mi, int me)
		{
			minInc = mi;
			maxExc = me;
		}
		
		public boolean evalSub(UserPack up)
		{
			int ncall = up.getBidCount();
			
			return (minInc <= ncall && ncall < maxExc);
		}
		
		public String toString()
		{
			return Util.sprintf("Callout Number min=%d max=%d", minInc, maxExc);	
		}		
		
		public FeatureCode getCode()
		{
			return FeatureCode.calloutcount;
		}	
		
		public Object[] getKeyList()
		{
			return new Object[] { minInc, maxExc };	
		}
		
		void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 2);
			minInc = Integer.valueOf(keylist[0]);
			maxExc = Integer.valueOf(keylist[1]);
		}	

	}
	
	public static class DomCatFeature extends UserFeature
	{
		String catName;
		Boolean isMode;
		Set<String> domset;
		
		DomCatFeature()
		{
			
		}
		
		DomCatFeature(String cn, boolean ism)
		{
			catName = cn;
			isMode = ism;
			domset = FeatureInfo.getDomainCats().get(catName);
		}
		
		public boolean evalSub(UserPack up)
		{
			if(isMode)
			{
				String dmode = up.getFieldMode(LogField.domain).trim();				
				return domset.contains(dmode);
				
			} else {
				
				for(BidLogEntry ble : up.getBidList())
				{
					String dom = ble.getField(LogField.domain).trim();
					
					if(domset.contains(dom))
					{ 
						//Util.pf("\nFound SINGLE domain %s for category %s", ble.getField("domain"), catName);
						return true; 
					}
				}		
				
				return false;
			}
		}
		
		public String toString()
		{
			return Util.sprintf("Domain Category %s - (%sMatch)", catName, (isMode ? "Mode" : "Single"));
		}		

		public FeatureCode getCode()
		{
			return FeatureCode.domain_category;
		}		
		
		public Object[] getKeyList()
		{
			return new Object[] { catName, isMode };	
		}
		
		void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 2);
			catName = keylist[0];
			isMode = Boolean.valueOf(keylist[1]);
			domset = FeatureInfo.getDomainCats().get(catName);			
		}	
	}
	
	public static class VarietyFeature extends UserFeature
	{
		LogField fname;
		Integer ntarg;
		Boolean isThresh;
		
		VarietyFeature()
		{
			
		}
		
		VarietyFeature(LogField f, int nt, boolean ist)
		{
			fname = f;
			ntarg = nt;
			isThresh = ist;
		}
		
		public boolean evalSub(UserPack up)
		{
			int div = up.getFieldDiversity(fname);
			
			if(isThresh)
				return div == ntarg;
			else
				return div < ntarg;
		}
		
		public String toString()
		{
			if(isThresh)
				return Util.sprintf("Distinct Values Field=%s Count=%d", fname, ntarg);
			else
				return Util.sprintf("Less Than %d Distinct Values For %s", ntarg, fname);
		}		
		
		public FeatureCode getCode()
		{
			return FeatureCode.surfing_behavior;	
		}	
		
		public Object[] getKeyList()
		{	
			return new Object[] { fname, ntarg, isThresh };
		}
		
		void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 3);
			fname = LogField.valueOf(keylist[0]);
			ntarg = Integer.valueOf(keylist[1]);
			isThresh = Boolean.valueOf(keylist[2]);
		}	
	}		
	
	public static class SingleMatch extends UserFeature
	{
		LogField _fieldName;
		String _targetVal;
				
		SingleMatch()
		{
			
			
		}
		
		SingleMatch(LogField fname, String targv)
		{
			_fieldName = fname;
			_targetVal = targv;
		}
		
		public boolean evalSub(UserPack up)
		{
			for(BidLogEntry ble : up.getBidList())
			{
				if(_targetVal.equals(ble.getField(_fieldName)))
					return true;
			}
			
			return false;			
		}
		
		public String toString()
		{
			return Util.sprintf("OneOrMoreMatch: %s=%s", _fieldName, _targetVal);	
		}
		
		public FeatureCode getCode()
		{
			return FeatureCode.valueOf(_fieldName.toString());
		}
		
		
			
		@Override
		public String getDisplayCat()
		{
			return _fieldName.toString();
		}		
		
		public Object[] getKeyList()
		{
			return new Object[] { _fieldName, _targetVal };	
		}
		
		void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 2);
			_fieldName = LogField.valueOf(keylist[0]);
			_targetVal = keylist[1];
		}	

	}
	
	public static class ModeMatch extends UserFeature
	{
		LogField _fieldName;
		String _targetVal;
		
		ModeMatch()
		{
			
		}
		
		ModeMatch(LogField fname, String targv)
		{
			_fieldName = fname;
			_targetVal = targv;
		}
		
		public boolean evalSub(UserPack up)
		{
			String highval = up.getFieldMode(_fieldName);
			
			return _targetVal.equals(highval);			
		}
		
		public String toString()
		{
			return Util.sprintf("FavoriteMatch: %s=%s", _fieldName, _targetVal);	
		}	
		
		public FeatureCode getCode()
		{
			return FeatureCode.valueOf(_fieldName.toString());
		}
		
		@Override
		public String getDisplayCat()
		{
			return _fieldName.toString();	
		}
		
		public Object[] getKeyList()
		{
			return new Object[] { _fieldName, _targetVal };	
		}
		
		void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 2);
			_fieldName = LogField.valueOf(keylist[0]);
			_targetVal = keylist[1];
		}			
	}
	
	// TODO: is this still necessary given that we now have IAB features....?
	public static class GoogVertFeat extends UserFeature
	{
		Integer targ;
		Boolean isMode;
		public static final LogField GOOG_VERT_ID = LogField.google_main_vertical;
		
		GoogVertFeat()
		{
			
		}
		
		GoogVertFeat(int tc, boolean ism)
		{
			targ = tc;
			isMode = ism;
		}
		
		public boolean evalSub(UserPack up)
		{
			if(isMode)
			{
				String mode = up.getFieldMode(GOOG_VERT_ID);
				
				if(mode.trim().length() == 0)
					{ return false;}
				
				return targ == Integer.valueOf(mode.trim());		
			}
			
			for(BidLogEntry ble : up.getBidList())
			{
				String vstr = ble.getField(GOOG_VERT_ID);
				
				if(vstr.trim().length() == 0)
					{ continue;}
				
				if(targ == Integer.valueOf(vstr))
					return true;
			}
			
			return false;
		}
		
		@Override
		public String toString()
		{
			String mtype = (isMode ? "ModeMatch" : "SingleMatch");
			String googcode = GoogleVertLookup.getSing().getFullCode(targ);
			googcode = Util.basicAsciiVersion(googcode);

			return Util.sprintf("GoogVertical %s of %s", mtype, googcode);				
		}	

		public FeatureCode getCode()
		{
			return FeatureCode.vertical;
		}
			
		public Object[] getKeyList()
		{
			return new Object[] { targ, isMode } ;
		}
		
		void buildFromKeyList(String[] keylist)
		{
			Util.massert(keylist.length == 2);
			targ = Integer.valueOf(keylist[0]);
			isMode = Boolean.valueOf(keylist[1]);
		}		
	}
}
