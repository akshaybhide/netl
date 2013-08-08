
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.LogType;
import com.adnetik.userindex.AdaBoost.BinaryFeature;

public class StrayerLook
{
	//private static final String ADA_SAVE_DIR = getCacheDir();
	
	//public static final List<AdaBoost.BinaryFeature<UserPack>> FEAT_LIST = StrayerFeat.getGoogVerts();
	public static SortedMap<String, BinaryFeature<UserPack>> _FEAT_MAP;
	//public static final List<AdaBoost.BinaryFeature<UserPack>> FEAT_LIST = Util.vector();
	
	// TODO: move this to StrayerFeat
	public static Map<String, BinaryFeature<UserPack>> getFeatMap()
	{
		if(_FEAT_MAP == null)
		{ 
			_FEAT_MAP = Util.treemap();
			
			for(BinaryFeature<UserPack> feature : StrayerFeat.getFeatList())
			{
				Util.massert(!_FEAT_MAP.containsKey(feature.toString()));	
				_FEAT_MAP.put(feature.toString(), feature); 
			}
			
			Util.pf("\nGenerated feature map, found %d features", _FEAT_MAP.size());
		}
			
		return _FEAT_MAP;
	}
	
	public static void main(String[] args) throws IOException
	{			
		
		if("clearcache".equals(args[0]))
		{
			clearCacheDir();	
			return;
		}		
		
		if("precomp".equals(args[0]))
		{
			//precompData(args);	
			return;
		}
		
		if("countusers".equals(args[0]))
		{
			UpackScanner upsc = new UpackScanner.SingleFile(args[1]);
			countUsers(upsc);
			return;
		}		
		
		if("genoutput".equals(args[0]))
		{
			generateOutput(args[1]);	
			return;
		}
		
		System.out.printf("\nargs 1 is %s %s %s", args[0], args[1], args[2]);
	}
		
	static void countUsers(UpackScanner upsc)
	{
		int ucount = 0;
		
		while(upsc.hasNext())
		{
			UserPack upack = upsc.next();
			ucount++;
			
			if(ucount % 100 == 0)
			{
				System.out.printf("\nUser count is %d, id is %s", ucount, upack.userId);
			}
		}
		
		System.out.printf("\nFinal User count is %d, got %d errors, %d empty IDs", 
			ucount, upsc.getExCount(), upsc.getNoIdCount());
	}
	
	static void generateOutput(String outputFile, int posPix, int negPix)
	throws IOException
	{
		generateOutput(outputFile, TimeUtil.getTodayCode(), posPix, negPix);
	}
	
	// Aggregates two precomputed pixel results into one batch, then generates the output.
	static void generateOutput(String outputFile, String daycode, int posPix, int negPix)
	throws IOException
	{
		File locposdir = new File(getLocalSaveDir(posPix));
		File locnegdir = new File(getLocalSaveDir(negPix));

		Util.pf("\nLoading precomputed data...");
		TreeMap<String, Vector<Boolean>> posmap = loadSinglePrecomp(locposdir);
		TreeMap<String, Vector<Boolean>> negmap = loadSinglePrecomp(locnegdir);
		Util.pf(" ... done");		
		
		int poscount = posmap.get(posmap.firstKey()).size();
		int negcount = negmap.get(negmap.firstKey()).size();
		
		Util.pf("\nFound %d positive, %d negative, %d total users", poscount, negcount, poscount+negcount);
		
		boolean[] targs = new boolean[poscount+negcount];
		for(int i = 0; i < targs.length; i++)
			{ targs[i] = (i < poscount); }
		
		Map<String, boolean[]> aggmap = Util.treemap();
		
		for(String funcname : posmap.keySet())
		{
			Util.setdefault(aggmap, funcname, new boolean[poscount+negcount]);	
			
			for(int i = 0; i < poscount; i++)
				{ aggmap.get(funcname)[i] = posmap.get(funcname).get(i); }
			
			for(int i = 0; i < negcount; i++)
				{ aggmap.get(funcname)[i+poscount] = negmap.get(funcname).get(i); }
		}
		
		generateOutput(outputFile, targs, aggmap);
	}
	
	// TODO: take this out, along with associated code.
	static void generateOutput(String outputFile) throws IOException
	{
		boolean[] targs = readSaveTargs();
		Map<String, boolean[]> pcMap = readTransPrecalc(targs.length);
	
		generateOutput(outputFile, targs, pcMap);
	}	
	
	
	static void generateOutput(String outputFile, boolean[] targs, Map<String, boolean[]> pcMap)
	throws IOException
	{
		PrintWriter pwrite = new PrintWriter(outputFile);
	
		System.out.printf("\nReading %d data objects and %d features",
				targs.length, pcMap.size());
		
		// Order high to low in terms of score
		SortedSet<Pair<Double, String>> sortfeat = new TreeSet<Pair<Double, String>>(Collections.reverseOrder());

		for(String funcName : getFeatMap().keySet())
		{
			AdaBoost.HitPack ahp = new AdaBoost.HitPack(pcMap.get(funcName), targs);
			Pair<Double, String> p = Pair.build(ahp.getMiScore(), funcName);
			sortfeat.add(p);
		}			

		// Make sure we didn't lose anything in the set adds
		Util.massert(sortfeat.size() == getFeatMap().size());
		
		for(Pair<Double, String> p : sortfeat)
		{
			String featname = p._2;
			AdaBoost.HitPack ahp = new AdaBoost.HitPack(pcMap.get(featname), targs);	
			featname = featname.replace(',', '_'); // for nicer CSV formatting
			ahp.writeInfo(pwrite, featname);
		}
				
		pwrite.close();
		
		Util.pf("\nFinished generating output.\n\n");			
	}
	
	// TODO: this initialization code is replicated with GenOutput
	// All of this should be package into a real object
	static AdaBoost<UserPack> learnClassifier(int posPix, int negPix)
	{
		File locposdir = new File(getLocalSaveDir(posPix));
		File locnegdir = new File(getLocalSaveDir(negPix));
		
		TreeMap<String, Vector<Boolean>> posmap = loadSinglePrecomp(locposdir);
		TreeMap<String, Vector<Boolean>> negmap = loadSinglePrecomp(locnegdir);
		
		int poscount = posmap.get(posmap.firstKey()).size();
		int negcount = negmap.get(negmap.firstKey()).size();
		
		Util.pf("\nFound %d positive, %d negative, %d total users", poscount, negcount, poscount+negcount);
		
		boolean[] targs = new boolean[poscount+negcount];
		for(int i = 0; i < targs.length; i++)
			{ targs[i] = (i < poscount); }
		
		Map<String, boolean[]> aggmap = Util.treemap();
		
		for(String funcname : posmap.keySet())
		{
			Util.setdefault(aggmap, funcname, new boolean[poscount+negcount]);	
			
			for(int i = 0; i < poscount; i++)
				{ aggmap.get(funcname)[i] = posmap.get(funcname).get(i); }
			
			for(int i = 0; i < negcount; i++)
				{ aggmap.get(funcname)[i+poscount] = negmap.get(funcname).get(i); }
		}
		
		return learnClassifier(targs, aggmap);
	}
	
	
	static AdaBoost<UserPack> learnClassifier(String cacheDir)
	{
		Vector<Boolean> targlist = FileUtils.unserializeEat(targSavePath(cacheDir));
		Map<String, boolean[]> pcMap = readTransPrecalc(targlist.size(), cacheDir);		
		
		boolean[] targs = vec2arr(targlist);
		
		int numPos = 0;
		for(int i = 0; i < targs.length; i++)
			{ numPos += (targs[i] ? 1 : 0); }
		
		Util.pf("\nLoaded cache data, found %d features and %d users, %d positive",
			pcMap.size(), targlist.size(), numPos);		
		
		return learnClassifier(targs, pcMap);
	}
	
	static AdaBoost<UserPack> learnClassifier(boolean[] targs, Map<String, boolean[]> pcMap)
	{
		AdaBoost<UserPack> adaclass = new AdaBoost<UserPack>(targs);
		
		for(int i = 0; i < 20; i++)
		{
			Util.pf("\nRunning boosting round %d ... ", i);
			adaclass.oneRoundPc(pcMap);
			Util.pf(" ... done");
			
			int numerr = adaclass.numErrs(pcMap, targs);
			Util.pf("\nNumber of errors is now %d", numerr);
		}
		
		return adaclass;
	}
	
	static boolean[] readSaveTargs()
	{
		String savePath = Util.sprintf("%s/precomp/targlist.ser", getCacheDir());
		Vector<Boolean> targvec = FileUtils.unserializeEat(savePath);
		
		boolean[] targs = new boolean[targvec.size()];
		
		for(int m = 0; m < targvec.size(); m++)
			{ targs[m] = targvec.get(m); }
		
		System.out.printf("\nTarget length is %d", targs.length);
		
		return targs;
	}
	
	static Map<String, boolean[]> readTransPrecalc(int targLen)
	{ return readTransPrecalc(targLen, getCacheDir()); }
	
	static Map<String, boolean[]> readTransPrecalc(int targLen, String cacheDir)
	{
		Util.pf("\nLoading cached feature values.... ");
		
		Map<String, boolean[]> pcMap = Util.treemap();		
		
		for(String funcName : getFeatMap().keySet())
		{
			String savePath = AdaBoost.featSavePath(cacheDir, funcName);
			Vector<Boolean> pvec = FileUtils.unserializeEat(savePath);
			
			Util.massertEq(pvec.size(), targLen);
			pcMap.put(funcName, new boolean[targLen]);
			
			for(int m = 0; m < pvec.size(); m++)
				{ pcMap.get(funcName)[m] = pvec.get(m); }
		}
		
		Util.pf(" ... done");
		
		return pcMap;
	}
	
	public static String getCacheDir()
	{
		String curdir = System.getProperty("user.dir");
		String[] dirtoks = curdir.split("/");
		
		int N = dirtoks.length;
		Util.massert(dirtoks[N-2].equals("burfoot"));
		String projcode = dirtoks[N-1];
		
		String cachedir = Util.sprintf("/tmp/userindex/adacache/%s", projcode);
		//Util.pf("\nUsing cache dir %s", cachedir);
		
		File gimp = new File(cachedir + "/precomp");
		gimp.mkdirs();
		
		return cachedir;
	}

	static void clearCacheDir() throws IOException
	{
		// TODO: this is dangerous...
		String cdir = getCacheDir();
		String syscall = Util.sprintf("rm -rf %s", cdir);
		Util.syscall(syscall);
		Util.pf("Deleted cache dir %s\n", cdir);
	}
	
	public static String getLocalSaveDir(int pixid)
	{
		return Util.sprintf("/mnt/data/tmp/burfoot/userindex/precomp/%s/pix_%s", 
							TimeUtil.getTodayCode(), Util.padLeadingZeros(pixid, 8));
	}

	public static String getUserFeaturePath(String posPix, String negPix)
	{
		return getUserFeaturePath(posPix, negPix, TimeUtil.getTodayCode());
	}			
	
	public static String getUserFeaturePath(String posPix, String negPix, String dayCode)
	{
		String featpath = Util.sprintf("/mnt/data/tmp/burfoot/userindex/feature_report/%s/report_%sx%s.csv", 
							dayCode, posPix, negPix);

		return featpath;		
	}		
	
	public static String getLocalAdaClassDir(String posPix, String negPix)
	{
		return getLocalAdaClassDir(posPix, negPix, TimeUtil.getTodayCode());
	}	

	public static String getLocalAdaClassDir(String posPix, String negPix, String daycode)
	{
		return Util.sprintf("/mnt/data/tmp/burfoot/userindex/adaclass/%s/pix_%sx%s", 
							daycode, posPix, negPix);
	}	
	
	
	public static void precompData(UpackScanner posPack, UpackScanner negPack, int maxCount)
	{
		
		System.out.printf("Precomputing feature responses for %d features, using maxCount=%d",
				getFeatMap().size(), maxCount);
		
		Vector<Boolean> targlist = Util.vector();
		Map<String, Vector<Boolean>> precompMap = Util.treemap();
		
		int foundCount = 0;
				
		for(String funcName : getFeatMap().keySet())
		{
			BinaryFeature<UserPack> feat = getFeatMap().get(funcName);
			String savePath = AdaBoost.featSavePath(getCacheDir(), funcName);	
			
			Vector<Boolean> prevres = Util.vector();
			if((new File(savePath)).exists()) {
				foundCount++;
				
				try { 
					Vector<Boolean> readdata = FileUtils.unserializeEat(savePath); 
					prevres.addAll(readdata);
				} catch (Exception ex) { 
					System.out.printf("\nwarning: error deserializing file %s", savePath);
				}
			}

			precompMap.put(feat.toString(), prevres);
		}
		
		System.out.printf("\nFound %d precomputed vectors", foundCount);
		int ucount = 0;
			
		while(posPack.hasNext() || negPack.hasNext())
		{
			boolean positive = posPack.hasNext();
			
			UserPack upack = (positive ? posPack : negPack).next();
			
			// TODO: insert min-callout size here.
			
			targlist.add(positive);
			
			for(String funcName : getFeatMap().keySet())
			{
				BinaryFeature<UserPack> feat = getFeatMap().get(funcName);
				Vector<Boolean> pvec = precompMap.get(funcName);
				
				// if we haven't built this up already, we are at most a single element behind.
				Util.massert(pvec.size() >= ucount);
				
				// We lose some performance here, but it's probably worth it as a sanity check
				if(Math.random() < 0.01 && pvec.size() > ucount)
				{
					boolean x = feat.eval(upack);
					Util.massert(x == pvec.get(ucount));
				}
				
				if(pvec.size() == ucount)
					{ pvec.add(feat.eval(upack)); }
			}
			
			//TODO: increase this to 100 or so
			if(ucount % 100 == 0)
			{
				System.out.printf("\nFinished for ucount=%d, userid=%s", ucount, upack.userId);
				savePrecompData(targlist, precompMap);
			}
			
			ucount++;			

		}
		
		System.out.printf("\nFinished, processed %d users total", ucount);
		savePrecompData(targlist, precompMap);	
	}
	
	// TODO: add reload capability
	public static void singlePrecomp(UpackScanner onepack, File saveDir, int maxCount)
	{
		System.out.printf("Precomputing feature responses for %d features, using maxCount=%d",
				getFeatMap().size(), maxCount);
		
		Vector<Boolean> targlist = Util.vector();
		TreeMap<String, Vector<Boolean>> precompMap = Util.treemap();
		
		// TODO: init pcmap by loading ...?
		
		int foundCount = 0;
			
		System.out.printf("\nFound %d precomputed vectors", foundCount);
		int ucount = 0;
			
		while(onepack.hasNext())
		{
			UserPack upack = onepack.next();	
			// TODO: insert min-callout size here.
			
			for(String funcName : getFeatMap().keySet())
			{
				BinaryFeature<UserPack> feat = getFeatMap().get(funcName);
				Util.setdefault(precompMap, funcName, new Vector<Boolean>());
				precompMap.get(funcName).add(feat.eval(upack));
			}
			
			if(ucount % 100 == 0)
			{
				System.out.printf("\nFinished for ucount=%d, userid=%s", ucount, upack.userId);
				savePrecompData(saveDir, precompMap);
			}
			
			ucount++;		
			
			if(ucount > maxCount)
				{ break; }	
		}
		
		System.out.printf("\nFinished, processed %d users total", ucount);
		savePrecompData(saveDir, precompMap);	
	}
		
	
	public static void precompData(String[] args) throws IOException
	{
		// TODO: should not even need to read these files, 
		// if all the feature data is already precomputed.
		
		if(2 > 1)
			{ throw new RuntimeException("Need to refactor this"); }
		
		System.out.printf("\nPrecomputing feature responses...");
		
		UpackScanner posPack = new UpackScanner.SingleFile(args[1]);
		UpackScanner negPack = new UpackScanner.SingleFile(args[2]);
		
		// TODO: do some kind of checking for overlapping user IDs...?
		precompData(posPack, negPack, Integer.MAX_VALUE);
	}

	private static TreeMap<String, Vector<Boolean>> loadSinglePrecomp(File precompDir)
	{
		String pcpath = bigPcMapPath(precompDir);		
		return FileUtils.unserializeEat(pcpath);		
	}
	
	private static void savePrecompData(File precompDir, TreeMap<String, Vector<Boolean>> precompMap)
	{		
		String pcpath = bigPcMapPath(precompDir);		
		FileUtils.serializeEat(precompMap, pcpath);
	}
	
	public static String bigPcMapPath(File precompDir)
	{
		return Util.sprintf("%s/big_precomp.ser", precompDir);
	}

	
	private static void savePrecompData(Vector<Boolean> targlist, Map<String, Vector<Boolean>> precompMap)
	{
		FileUtils.serializeEat(targlist, targSavePath()); 
		
		for(String funcName : precompMap.keySet())
		{
			String savePath = AdaBoost.featSavePath(getCacheDir(), funcName);			
			FileUtils.serializeEat(precompMap.get(funcName), savePath);
		}				
	}
	
	
	private static String targSavePath()
	{
		return targSavePath(getCacheDir());	
	}
	
	private static String targSavePath(String cacheDir)
	{
		return Util.sprintf("%s/precomp/targlist.ser", cacheDir);	
	}
	
	public static void listFields(Collection<UserPack> users, String fCode)
	{
		Set<String> regions = Util.treeset();
		
		for(UserPack user : users)
		{
			for(BidLogEntry ble : user.getData())
			{
				regions.add(ble.getField(fCode));
			}
		}
		
		for(String s : regions)
		{
			System.out.printf("\n\tflist.add(\"%s\");", s);	
		}
	}	
	

	
	// TODO: is this still necessary?
	public static Set<String> readTargIdSet(String pathTarg) throws IOException
	{
		Set<String> idset = Util.treeset();
		int targLen = -1;		
		
		for(String oneline : FileUtils.readFileLines(pathTarg))
		{
			String id = oneline.trim();
			Util.massert(!idset.contains(id), "ID already present in id set %s", id);
			
			if(targLen == -1)
				{ targLen = id.length(); }
			else
				{ Util.massert(id.length() == targLen, "Nonstandard length %d for id=%s", id.length(), id); }
			
			idset.add(id);			
		}
		
		return idset;
	}
	
	public static void testFeatures(List<BinaryFeature<UserPack>> featList, Collection<UserPack> users)
	{
		for(BinaryFeature<UserPack> feat : featList)
			{ testFeature(feat, users); }
	}
	
	public static void testFeature(BinaryFeature<UserPack> feat, Collection<UserPack> users)
	{
		int fcount = 0;
		
		for(UserPack user : users)
		{
			if(feat.eval(user))
			{	
				fcount++;
				//System.out.printf("\nFires for user %s", user.userId);
				//user.printMe();
				//System.out.printf("\nFeature fires for: %s", user);
			}
		}
		
		System.out.printf("\nFeature %s fires for %d users out of %d", feat, fcount, users.size());
	}
	
	public static void showDomains(List<UserPack> users)
	{
		for(UserPack user : users)
		{
			for(BidLogEntry ble : user.getData())
			{
				String domain = ble.getField("domain");
				
				if(domain == null && ble.getField("url").length() > 0)
				{
					System.out.printf("\nCould not find domain for url=%s", ble.getField("url"));
				}
			}
		}
	}	
	
	public static void showTopDomains(List<UserPack> users)
	{
		Map<String, Integer> countMap = Util.treemap();
		
		for(UserPack user : users)
		{
			for(BidLogEntry ble : user.getData())
			{
				String domain = ble.getField("domain");
				if(domain == null)
					{ continue; }
				
				Util.incHitMap(countMap, domain);
			}
			
		}
		
		SortedMap<Double, String> invMap = Util.invHitMap(countMap);

		int topCount = 0;
		
		for(Double k : invMap.keySet())
		{
			topCount++;
			//System.out.printf("\nDomain %s has score %.01f", invMap.get(k), k);
			
			System.out.printf("\n\tdlist.add(\"%s\");", invMap.get(k));
			
			if(topCount > 100)
				{ break; }
		}
		
	}	
	
	static boolean[] vec2arr(Vector<Boolean> t)
	{
		boolean[] x = new boolean[t.size()];
		
		for(int i = 0; i < t.size(); i++)
			{ x[i] = t.get(i); }
		
		return x;
	}
}
