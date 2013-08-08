
package com.adnetik.pricing;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;

public class InitialTest
{

	Map<String, Double> sumMap = Util.treemap();
	BasicLinReg basicReg = new BasicLinReg();
	
	int bleCount = 0;
	
	Map<String, ImpFeature> featMap = getFeatMap();
	
	public static String TEST_FILE = "/mnt/data/burfoot/pricing/head_imp_file.txt";

	public static void main(String[] args) throws Exception
	{
		InitialTest itest = new InitialTest();
		
		for(int i = 0; i < 5; i++)
		{
			double sqsum = itest.oneScan();
			
			Util.pf("\nAfter it %d, sqsum is %.03f", i, sqsum);
			
			itest.updateReg();
		}
	}
	
	String getBestFeat()
	{
		String bestfeat = "";
		double bestcorr = -10000000;
		
		for(String featname : sumMap.keySet())
		{
			double c = Math.abs(sumMap.get(featname));
			
			//Util.pf("\nCorrelation for %s is %.03f", featname, c);
			
			if(c > bestcorr)
			{
				bestfeat = featname;	
				bestcorr = c;
			}
		}
		
		return bestfeat;
	}
	
	
	void updateReg()
	{
		String bestfeat = getBestFeat();
		double bestcorr = sumMap.get(bestfeat);
		
		double wait = bestcorr / bleCount;
		
		Util.pf("\nBest feat is %s, best corr is %.03f", bestfeat, bestcorr);
		
		basicReg.addFeature(featMap.get(bestfeat), wait);
				
	}
	
	double oneScan() throws Exception
	{
		double sqsum = 0;
		bleCount = 0;
		
		sumMap.clear();
		
		for(String featname : featMap.keySet())
			{ sumMap.put(featname, 0D); }
		
		Scanner sc = new Scanner(new File(TEST_FILE));
		
		while(sc.hasNextLine())
		{
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v13, sc.nextLine());	
			
			double res = basicReg.getResidual(ble);
			
			//Util.pf("\nResidual is %.03f",res);
			
			sqsum += res*res;
			bleCount += 1;

			for(String featname : featMap.keySet())
			{
				double prv = sumMap.get(featname);
				
				double diff = featMap.get(featname).evali(ble) * res;
				
				sumMap.put(featname, prv+diff);
			}
			
			if((bleCount % 10000) == 0)
			{
				Util.pf("\nFinished with ble index %d", bleCount);
				
			}
		}	
		
		return sqsum;
	}
	
	
	public static Map<String, ImpFeature> getFeatMap()
	{
		List<ImpFeature> flist = Util.vector();
		
		flist.add(new ImpFeature.NullFeat());
		
		flist.add(new ImpFeature.SocialMedia());
		
		for(int i = 0; i < 24; i++)
			{ flist.add(new ImpFeature.HourOfDay(i)); }
		
		
		String[] browser = new String[] { "MSIE", "Chrome", "Firefox", "Safari" };
		
		for(String oneb : browser)
			{ flist.add(new ImpFeature.BrowserFeat(oneb)); }
		
		
		Map<String, ImpFeature> featmap = Util.treemap();
		
		for(ImpFeature impf : flist)
			{ featmap.put(impf.getCode(), impf); }

		return featmap;
	}
}
