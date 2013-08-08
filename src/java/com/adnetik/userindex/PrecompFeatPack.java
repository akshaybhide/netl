
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
import com.adnetik.shared.Util.LogType;
import com.adnetik.userindex.UserIndexUtil.*;

public class PrecompFeatPack implements Serializable
{
	// uuid, country, region
	public static final int START_FIELDS = 3;
	
	TreeMap<String, Vector<Boolean>> precompMap = Util.treemap();
	
	int newEvals = 0;
		
	private String _blockEnd;

	ScanRequest _scanReq;
	
	// Use type safe one with ScanRequest
	@Deprecated
	public PrecompFeatPack(String blockend, String listcode)
	{
		UserIndexUtil.assertValidBlockEnd(blockend);
		_blockEnd = blockend;
		
		Util.massert(ListInfoManager.getSing().haveRequest(listcode),
			"Could not find listcode %s", listcode);
		
		_scanReq = ListInfoManager.getSing().getRequest(listcode);
	}
	
	public PrecompFeatPack(String blockend, ScanRequest screq)
	{
		UserIndexUtil.assertValidBlockEnd(blockend);
		_blockEnd = blockend;
		_scanReq = screq;
	}	
	
	public String getBlockEnd()
	{
		return _blockEnd;	
	}
	

	
	public boolean precompFileExists() throws IOException
	{
		Path pcpath = new Path(UserIndexUtil.getPrecompHdfsPath(_blockEnd, _scanReq));
		FileSystem fsys  = FileSystem.get(new Configuration());
		if(!fsys.exists(pcpath))
			{ return false; }
		
		if(fsys.getFileStatus(pcpath).getLen() == 0)
			{ return false; }
		
		return true;
	}
	
	public void loadFromHdfs() throws IOException
	{
		loadFromHdfs(UserIndexUtil.MAX_USERS_FEATURE);
	}	
	
	public void loadFromHdfs(int maxusers) throws IOException
	{
		if(!UFeatManager.isSingReady())
			{  UFeatManager.initSing(_blockEnd); }
		
		String pcpath = UserIndexUtil.getPrecompHdfsPath(_blockEnd, _scanReq);
		Configuration mconf = new Configuration();
		BufferedReader bread = HadoopUtil.hdfsBufReader(FileSystem.get(mconf), pcpath);
		loadFromReader(bread, maxusers);
	}

	private void loadFromReader(BufferedReader bread, int maxUsers) throws IOException 
	{
		List<String> funcnames = Util.vector();
		for(int i = 0; i < START_FIELDS; i++)
			{ funcnames.add("unused"); }
		
		for(UserFeature ufeat : UFeatManager.getSing().getFeatSet(_scanReq))
		{
			String funcname = ufeat.getNameKey();
			funcnames.add(funcname);
			precompMap.put(funcname, new Vector<Boolean>());
		}
		
		int ucount = 0;
		for(String logline = bread.readLine(); logline != null; logline = bread.readLine())
		{
			String[] toks = logline.split("\t");
			
			Util.massert(toks.length == funcnames.size(), 
				"Found %d tokens but %d functions for scanreq %s", 
				toks.length, funcnames.size(), _scanReq.getListCode());
			
			// Util.pf("Found %d tokens and %d functions\n", toks.length, funcnames.size());
			
			for(int t = START_FIELDS; t < toks.length; t++)
			{
				Integer val;
				if(toks[t].indexOf("=") > -1)
				{
					// random alignment check
					String[] code_val = toks[t].split("=");
					int codeA = UserIndexUtil.funcCode(funcnames.get(t));
					int codeB = Integer.valueOf(code_val[0]);
					
					//Util.pf("For funcname=%s, codeA=%d, codeB=%d\n",
					//	funcnames.get(t), codeA, codeB);
					
					// There is something screwy with these error checks for
					// non-basic ascii strings (accents etc) so let's just skip
					Util.massert(codeA == codeB || Util.hasNonBasicAscii(funcnames.get(t)) || (funcnames.get(t).indexOf("improvedigital") > -1), 
						"Error in code check for %s, codeA=%d codeB=%d", funcnames.get(t), codeA, codeB);
					
					// Util.pf("Random alignment check passed for %s=%d\n", funcnames.get(t), codeA);
					
					val = Integer.valueOf(code_val[1]);
				} else {
					val = Integer.valueOf(toks[t]);	
				}
				// all this work just for a goddam bit
				boolean funcval = (val == 1);
				precompMap.get(funcnames.get(t)).add(funcval);
			}
			
			ucount++;
			
			if((ucount % 400) == 0)
			{
				Util.pf("Finished reading map for ucount=%d, id=%s\n", ucount, toks[0]);
			}
			
			if(ucount >= maxUsers)
				{ break; }
		}		
		
		bread.close();
	}
	
	public static PrecompFeatPack evalUserPack(UpackScanner pack, int maxcount, String daycode, String listid)
	{
		PrecompFeatPack pfp = new PrecompFeatPack(daycode, listid);
		pfp.addEvalFromPack(pack, maxcount);
		return pfp;
	}
	
	public void addEvalFromPack(UpackScanner pack, int maxCount)
	{
		double startTime = System.currentTimeMillis();
		
		for(int ucount = 0; ucount < maxCount; ucount++)
		{
			if(!pack.hasNext())
				{ break; }
			
			UserPack upack; 
			
			try { upack = pack.next(); }
			catch (IOException ioex) { continue; }
			
			for(UserFeature ufeat : UFeatManager.getSing().getFeatSet(_scanReq))
			{
				String funcname = ufeat.toString();
				Util.setdefault(precompMap, funcname, new Vector<Boolean>());
				//Util.pf("\nmap is %s, funcname is %s", precompMap, funcname);
				Util.massert(precompMap.get(funcname).size() >= ucount);
				
				if(precompMap.get(funcname).size() == ucount)
				{
					precompMap.get(funcname).add(ufeat.eval(upack).getVal());
					newEvals++;					
				}
			}		
			
			if((ucount+1) % 100 == 0)
			{
				Util.pf("\nFinished for ucount=%d, userid=%s, time is %.02f seconds, newevals=%d", 
					ucount+1, upack.userId, (System.currentTimeMillis() - startTime)/1000, newEvals);
				
				// Don't save here, so we can avoid screwing up the serialized file
				// if(newEvals > 0)  { saveToDisk(); }
			}
		}

		Util.pf("\nFinished for all users, took %.02f seconds", 
			(System.currentTimeMillis() - startTime)/1000);
		
		// saveToDisk();	
	}	
	
	public int numUsers()
	{
		if(precompMap.isEmpty())
			{ return 0; }
		
		return precompMap.get(precompMap.firstKey()).size();
	}
	
	public int numFeatures()
	{
		return precompMap.size();	
	}
	
	public static boolean[] genTargVec(PrecompFeatPack pos, PrecompFeatPack neg)
	{
		boolean[] tvec = new boolean[pos.numUsers() + neg.numUsers()];
		
		for(int i = 0; i < pos.numUsers(); i++)
			{ tvec[i] = true; }
		
		return tvec;
	}

	public static TreeMap<String, boolean[]> combine(PrecompFeatPack poscalc, PrecompFeatPack negcalc)
	{
		// Null - include all features
		return combine(poscalc, negcalc, null);	
	}
	
	// Combine two PFPs together into a function map that can be used for learning
	// Only use features with the feature codes in the given set
	public static TreeMap<String, boolean[]> combine(PrecompFeatPack poscalc, PrecompFeatPack negcalc, Set<FeatureCode> includeset)
	{
		int poscount = poscalc.numUsers();
		int negcount = negcalc.numUsers();
		
		TreeMap<String, Vector<Boolean>> posmap = poscalc.precompMap;
		TreeMap<String, Vector<Boolean>> negmap = negcalc.precompMap;
		TreeMap<String, boolean[]> boolmap = Util.treemap();
		
		// Positive Set must be SUBSET of Negative Set
		Util.massert(negmap.keySet().containsAll(posmap.keySet()),
			"Neg map must be full superset of posmap");
				
		// Combine two precalc features together
		for(String namekey : posmap.keySet())
		{
			UserFeature ufeat = UserFeature.buildFromNameKey(namekey);
			
			// If we specify an include-set, DQ features that aren't on it
			if(includeset != null && !includeset.contains(ufeat.getCode()))
				{ continue; }
						
			Util.setdefault(boolmap, namekey, new boolean[poscount+negcount]);	
			
			for(int i = 0; i < poscount; i++)
				{ boolmap.get(namekey)[i] = posmap.get(namekey).get(i); }
			
			for(int i = 0; i < negcount; i++)
				{ boolmap.get(namekey)[i+poscount] = negmap.get(namekey).get(i); }
		}		

		return boolmap;
	}
	
	private void checkMapLen()
	{
		if(precompMap.isEmpty())
			{ return; }
		
		TreeSet<Integer> checkset = Util.treeset();
		
		// JClose!!!
		for(String fname : precompMap.keySet())
		{
			checkset.add(precompMap.get(fname).size());	
		}
		
		Util.massert(checkset.size() == 1, "Precomputed vectors have nonstandard lengths %s", checkset);
		Util.pf("\nVector lengths okay, all have length=%d, %d features", checkset.first(), precompMap.size());
	}
		
	public static void main(String[] args) throws IOException
	{
		PrecompFeatPack fcalc = new PrecompFeatPack("2013-03-17", "pixel_8600");
		fcalc.loadFromHdfs(1100);
		
		for(String fname : fcalc.precompMap.keySet())
		{
			Util.pf("FNAME is %s\n", fname);	
			
		}
	}
	
}
