
package com.adnetik.userindex;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

public class LocalScoreUser 
{	
	// Pixel ID --> classifier
	Map<String, AdaBoost<UserPack>> _classifMap;
	
	Map<String, boolean[]> _funcMap = Util.treemap();
	
	Random _sanRand = new Random();	
	
	FileSystem _fSystem;
	
	SortedFileMap _sortMap;
	
	private int _zerCountUser = 0;
	private int _oneCountUser = 0;
	private int _twoCountUser = 0;
	private int _thrCountUser = 0;
	private int _maxCountUser = 0;
	
	private int _bidCount = 0;
	private int _pixCount = 0;
	
	private int _userCount = 0;
	
	private static final int MAX_QUEUE_SIZE = 100000;
	
	
	public static void main(String[] args) throws Exception
	{
		LocalScoreUser lsu = new LocalScoreUser("2013-04-14");
		lsu.doScoring();
	}
	
	public LocalScoreUser(String blockend) throws IOException
	{
		UserIndexUtil.assertValidBlockEnd(blockend);
		
		_fSystem = FileSystem.get(new Configuration());
		
		String prevblock = TimeUtil.nDaysBefore(blockend, 7);
		_classifMap = UserIndexUtil.readClassifData(_fSystem, prevblock);
		
		
		for(String listcode : _classifMap.keySet())
		{
			for(String onefunc : _classifMap.get(listcode).getBaseNameKeyList())
			{
				// evaluating one user at a time, so only really need one-element array
				_funcMap.put(onefunc, new boolean[1]);	
			}
		}
		
		Util.pf("Read %d AdaBoost classifiers\n", _classifMap.size());
		
		// False=nosort, must sort beforehand
		BufferedReader bread = FileUtils.getReader("sortperfdata.txt");
		
		// Peelfirst=true, means we don't want the first column to be 
		// part of the line data
		_sortMap = new SortedFileMap(bread, MAX_QUEUE_SIZE, 0, true);
	} 
	
	void doScoring() throws IOException
	{
		double startup = Util.curtime();
		
		Util.NO_SPRINTF = true;
		
		while(!_sortMap.isEmpty())
		{
			Map.Entry<String, List<String>> userentry = _sortMap.pollFirstEntry();
			scoreOneUser(userentry.getKey(), userentry.getValue().iterator());
			
			_userCount++;
			
			if((_userCount % 1000) == 0)
			{
				double upersec = (_userCount / ((Util.curtime()-startup)/1000));
				
				Util.pf("Finished scoring %d users, took %.03f, avg user/sec=%.03f\n",
					_userCount, (Util.curtime()-startup)/1000, upersec);
			}
			
		}
		
		Util.pf("Finished with scoring, stats are: %d 0count, %d 1count, %d 2count\n",
			_zerCountUser, _oneCountUser, _twoCountUser);
		
		Util.pf("Finished with scoring, stats are: %d 3count, %d maxcount, %d bid, %d pix\n",
			_thrCountUser, _maxCountUser, _bidCount, _pixCount);	
	
	}
	
	// This signature is modelled after the one in the main Hadoop job
	void scoreOneUser(String key , Iterator<? extends Object> values) throws IOException
	{
		{
			WtpId widkey = WtpId.getOrNull(key.toString());
			if(widkey == null)
				{ return; }
		}
		
		
		try {
			// Keys are just WTP-Ids.
			UserPack curpack = new UserPack();
			curpack.userId = key.toString();
			
			ScoreUserJob.popUPackFrmRdcrData(curpack, values, null);
			
			// Log some info about how many callouts/user
			{
				int cocount = curpack.getBidList().size();	
				
				if(cocount == 1) { _oneCountUser++; }
				if(cocount == 2) { _twoCountUser++; }
				if(cocount == 3) { _thrCountUser++; }	
				
				if(cocount == 0)
				{
					// This should almost never happen
					_zerCountUser++;
					return;						
				}
			}
			
			String ucountry = curpack.getFieldMode(LogField.user_country);
			Util.massert(ucountry != null && ucountry.length() == 2,
				"Bad user country '%s'", ucountry);	
			
			// The idea here is to calculate the feature results ONCE, and use those 
			// feature results to compute user scores for each classifier,
			// since all the classifiers are built on the same feature set.
			for(String namekey : _funcMap.keySet())
			{
				UserFeature onefeat = UserFeature.buildFromNameKey(namekey);
				_funcMap.get(namekey)[0] = onefeat.eval(curpack).getVal();
			}
			
			// Okay, this method is a bit inefficient, because we are 
			// evaluating the features 
			for(Map.Entry<String, AdaBoost<UserPack>> lc_ada : _classifMap.entrySet())
			{
				// PosRequest preq = (PosRequest) ScanRequest.buildFromListCode(listcode);
				
				String listcode = lc_ada.getKey();
				AdaBoost<UserPack> ada = lc_ada.getValue();
				double score = ada.calcScore(_funcMap, 0); // 0 = index of user, since we only have one user
				
				// Sanity check - this method is inefficient but doesn't depend on doing 
				// the whole funcMap prefix checking stuff correctly.
				if(_sanRand.nextDouble() < .00001)
				{
					List<String> basekeylist = ada.getBaseNameKeyList();
					double checkscore = ada.calcScore(UserFeature.buildFeatMap(basekeylist), curpack);
					Util.massert(Math.abs(score - checkscore) < 1e-9);					
				}
				
				// apparently sometimes we can get infinite scores
				if(Math.abs(score) > 1e8)
				{
					// reporter.incrCounter(ScoreCounter.InfScores, 1);
					continue;
				}
				
				// String resval = Util.sprintf("%s\t%.05f\t%s", listcode, score, ucountry);
				// collector.collect(key, new Text(resval));
			}
			
			_bidCount += curpack.getBidCount();
			_pixCount += curpack.getPixCount();
		} 
		
		catch (Exception ex) {
			
			throw new RuntimeException(ex);
			
			//reporter.incrCounter(HadoopUtil.Counter.GenericError, 1);
		}
	}		
}		

