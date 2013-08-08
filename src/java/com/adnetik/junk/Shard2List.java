
package com.adnetik.userindex;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

// This is a utility that generates user lists from Shard data created by ScoreUserJob
// Call using hadoop -jar /userindex/userscores/shard_*/part* | Shard2List
public class Shard2List
{
	Map<String, SortedSet<Pair<Double, WtpId>>> bigMap = Util.treemap();
	LinkedList<Double> timeList = Util.linkedlist();
	
	public static void main(String[] args) throws Exception
	{
		Shard2List s2l = new Shard2List();
		s2l.readData();
		s2l.writeLists();
	}
	
	void readData() throws IOException
	{
		timeList.add((double) Util.curtime());
		
		int lcount = 0; int ucount = 0;
		WtpId curid = new WtpId(Util.WTP_ZERO_ID);
		BufferedReader bread = new BufferedReader(new InputStreamReader(System.in));
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String[] toks = oneline.split("\t");
			WtpId id = new WtpId(toks[0]);
			String listcode = toks[1];
			Double score = Double.valueOf(toks[2]);
			
			if(id.compareTo(curid) != 0)
			{
				ucount++;	
				curid = id;
			}
			
			if(!bigMap.containsKey(listcode))
			{
				Util.pf("Found new listcode %s\n", listcode);
				SortedSet<Pair<Double, WtpId>> oneset = Util.treeset();
				bigMap.put(listcode, oneset);
			}
			
			SortedSet<Pair<Double, WtpId>> relset = bigMap.get(listcode);
			
			if(relset.size() < UserIndexUtil.FINAL_LIST_SIZE || score > relset.first()._1)
			{
				Pair<Double, WtpId> x = Pair.build(score, id);
				relset.add(x);
				
				while(relset.size() > UserIndexUtil.FINAL_LIST_SIZE)
				{
					relset.remove(relset.first());	
				}
			}
			
			if(((++lcount) % 100000) == 0)
			{
				double tooktime = (Util.curtime() - timeList.getLast())/1000;
				Util.pf("Finished reading line %d, scored %d users, took %.02f secs\n", lcount, ucount, tooktime);
				timeList.add((double) Util.curtime());
			}
		}
		
		bread.close();
	}
	
	void writeLists()
	{
		for(String listcode : bigMap.keySet())
		{
			Util.pf("Writing %d users for list %s\n", bigMap.get(listcode).size(), listcode);
			List<String> flist = Util.vector();
			
			for(Pair<Double, WtpId> onepair : bigMap.get(listcode))
			{
				flist.add(onepair.toString());
				//flist.add(onepair._2.toString());
			}
			
			FileUtils.writeFileLines(flist, listcode + ".list");
		}
	}
}



