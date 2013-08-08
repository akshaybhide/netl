
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.userindex.UserIndexUtil.*;

public class AdaBoost<X> implements Serializable
{
	public static final String REVERSE_PREF = "ReverseOf";
	
	// Alpha values. Length of list is number of boosting rounds.
	Vector<Double> alpha = Util.vector();
	
	// List of function names in order
	Vector<String> funcs = Util.vector();
	
	// Weighted distribution of samples.
	double[] Dt; 
	
	// Number of objects - shorthand for targets.length
	int M;
		
	boolean[] targets;
	
	// private List<X> trainData = new Vector<X>();
		
	// Function index, error, and prediction of best predictor found so far
	String bestF = null;
	double bestErr = 1e20;
	boolean[] bestPred = null;

	public AdaBoost()
	{
		
		
	}

	public AdaBoost(boolean[] targs)
	{
		M = targs.length;
		
		// Initial weights are 1/M
		{
			Dt = new double[M];
			double initVal = 1.0;
			initVal /= M;
			Arrays.fill(Dt, initVal);
		}

		targets = new boolean[M];
		bestPred = new boolean[M];		
		
		System.arraycopy(targs, 0, targets, 0, M);		
	}
	
	// Just returns the "real" functions names, without the reverse prefix.
	public List<String> getBaseNameKeyList()
	{
		// Argh Jclose!!!
		List<String> real = Util.vector();
		
		for(String onefunc : funcs)
		{
			if(onefunc.startsWith(REVERSE_PREF)) {
				real.add(Util.splitOnFirst(onefunc, "\t")._2);
			} else  {
				real.add(onefunc);	
			}
		}
		
		return real;
	}
	
	public int numFuncs()
	{
		return alpha.size();	
	}
	
	// Returns the "base" name key for the UserFeature
	String getBaseNameKey(int t)
	{
		String nk = funcs.get(t);
		
		if(nk.startsWith(REVERSE_PREF))
			{ return Util.splitOnFirst(nk, "\t")._2; }

		return nk;
	}
	
	boolean isReverse(int t)
	{
		return funcs.get(t).startsWith(REVERSE_PREF);
	}
	
	double getAlphaVal(int t)
	{
		return alpha.get(t);
	}
	
	private double calcScore(boolean[] featureVals)
	{
		double score = 0;
		
		for(int t = 0; t < alpha.size(); t++)
		{
			int ht = b2i(featureVals[t]);
			score += (ht * alpha.get(t));
		}
		
		return score;			
	}
	
	public double calcScore(Map<String, boolean[]> funcMap, int dataInd)
	{
		boolean[] featvals = new boolean[funcs.size()];
		
		for(int t = 0; t < alpha.size(); t++)
		{
			// okay just do reverse here
			String basenamekey = getBaseNameKey(t);
		
			// this is paranoid error checking, to make sure 
			// I'm using NameKeys correctly. Maybe take out if it's slowing things down
			{ 
				UserFeature ufeat = UserFeature.buildFromNameKey(basenamekey);
			}
			
			featvals[t] = funcMap.get(basenamekey)[dataInd];
			
			if(isReverse(t))
				{ featvals[t] = !featvals[t]; }
		}
		
		return calcScore(featvals);		
	}
	
	public String lastBestFunc()
	{
		return funcs.get(funcs.size()-1);
	}
	
	public double calcScore(Map<String, ? extends BinaryFeature<X>> funcMap, X dataObj)
	{
		boolean[] featvals = new boolean[alpha.size()];
		
		for(int t = 0; t < alpha.size(); t++)
		{
			// okay just do reverse here
			String basefuncname = getBaseNameKey(t);
			
			BinaryFeature<X> bfeat = funcMap.get(basefuncname);
			
			boolean funcresult = bfeat.eval(dataObj).getVal();
			
			featvals[t] = (isReverse(t) ? !funcresult : funcresult);
		}
		
		return calcScore(featvals);	
	}	
	
	public boolean classify(Map<String, boolean[]> funcMap, int dataInd)
	{
		return (calcScore(funcMap, dataInd) > 0);
	}
	
	public boolean classify(Map<String, BinaryFeature<X>> funcMap, X dataObj)
	{
		return (calcScore(funcMap, dataObj) > 0);
	}	
	
	public int numErrs(Map<String, BinaryFeature<X>> funcMap, List<X> data, boolean[] targs)
	{
		int numErr = 0;
		
		for(int i = 0; i < data.size(); i++)
		{
			X dataObj = data.get(i);
			
			boolean guess = classify(funcMap, dataObj);
			boolean crrct = (guess == targs[i]);
			
			numErr += (crrct ? 0 : 1);
		}
		
		return numErr;
	}
	
	public int numErrs(Map<String, boolean[]> precompMap, boolean[] targs)
	{
		int numErr = 0;
		
		for(int i = 0; i < targs.length; i++)
		{			
			boolean guess = classify(precompMap, i);
			boolean crrct = (guess == targs[i]);
			
			numErr += (crrct ? 0 : 1);
		}
		
		return numErr;
	}	
		
	private double alphaCalc(double eps)
	{
		return .5 * Math.log((1 - eps)/eps);
	}
	
	public static int b2i(boolean x)
	{
		return (x ? 1 : -1);	
	}


	// Pc = precomputed values
	public double oneRoundPc(Map<String, boolean[]>  precalcMap)
	{
		boolean check = false;
		//Util.massertEq(precalcMap.get(precalcMap.firstKey()).length, M);
		
		for(String fName : precalcMap.keySet())
		{
			if(!check)
			{
				Util.massertEq(precalcMap.get(fName).length, M);
				check = true;	
			}
			
			checkBest(precalcMap.get(fName), fName);
		}

		return doUpdate();
	}		
	
	// Run using the feature functions themselves
	public double oneRound(SortedMap<String, BinaryFeature<X>> funcMap, List<X> trainData)
	{
		for(String fname : funcMap.keySet())
		{
			boolean[] pred = new boolean[M];
			BinaryFeature<X> feat = funcMap.get(fname);

			for(int i = 0; i < M; i++)
			{
				pred[i] = feat.eval(trainData.get(i)).getVal();
			}
			
			checkBest(pred, fname);
		}
		
		return doUpdate();
	}
	
	private void checkBest(boolean[] htpred, String fName)
	{
		double wErr = 0;
		
		for(int i = 0; i < M; i++)
		{
			boolean correct = (htpred[i] == targets[i]);
			wErr += (correct ? 0 : Dt[i]);
		}		
			
		if(wErr < bestErr)
		{
			bestErr = wErr;
			bestF = fName;			
			System.arraycopy(htpred, 0, bestPred, 0, M);
			//Util.pf("\nFound best error of %.03f for feature %s", bestErr, bestF);
		} 
		else if((1 - wErr) < bestErr)
		{
			// Reverse Feature.
			bestErr = (1 - wErr);
			bestF = REVERSE_PREF + "\t" + fName;
			
			for(int i = 0; i < M; i++)
				{ bestPred[i] = !htpred[i]; }
			
			//Util.pf("\nFound best error of %.03f for feature %s", bestErr, bestF);
		}
	}
	
	private double doUpdate()
	{				
		double a = alphaCalc(bestErr);
		
		alpha.add(a);
		funcs.add(bestF);
		
		double Zt = 0; // norm factor
		
		for(int i = 0; i < M; i++)
		{
			// If the prediction is CORRECT, decay the weight by exp(-a), otherwise don't change it.
			double expon = (b2i(bestPred[i]) * b2i(targets[i])); 
			Dt[i] *= Math.exp(-a * expon);

			// For normalization 
			Zt += Dt[i];
		}
		
		for(int i = 0; i < M; i++)
		{
			Dt[i] /= Zt;

			//System.out.printf("\nFor user=%d, best pred is %b vs targ %b",
			//		i, bestPred[i], targets[i]);
			//System.out.printf("\nWeight dist for %d is %.03f", i, Dt[i] * M);
		}
		
		//System.exit(1);
		
		double toReturn = bestErr;
		
		// Clean up
		bestErr = 1e20;
		bestF = null;
		
		return toReturn;
	}
	
	public Map<Double, Integer> mutualInfoScore(boolean[][] precalc)
	{
		SortedMap<Double, Integer> scoreMap = new TreeMap<Double, Integer>(Collections.reverseOrder());
				
		for(int pci = 0; pci < precalc.length; pci++)
		{
			//System.out.printf("\nScoring for pci=%d", pci);
			HitPack hp = new HitPack(precalc[pci], targets);
			
			double miscore = hp.getMiScore();
			
			while(scoreMap.containsKey(miscore))
			{ 
				miscore -= 1e-7;
				//System.out.printf("\nScore map hit... for score %.03f", miscore);
			}	
			
			scoreMap.put(miscore, pci);
		}
		
		return scoreMap;
	}	

	// Returns a list of strings that represent a serialized form of the classifier
	public List<String> getSaveListInfo()
	{
		List<String> reclist = Util.vector();
		for(int t = 0; t < alpha.size(); t++)
		{ 
			String recline = Util.sprintf("%.05f\t%s", alpha.get(t), funcs.get(t)); 
			reclist.add(recline);
		}
		return reclist;
	}
	
	void readSaveListInfo(List<String> saveinfolist)
	{
		Util.massert(funcs.isEmpty() && alpha.isEmpty(),
			"Attempt to read save list data in already initialized classifier");
		
		for(String onerec : saveinfolist)
		{
			Pair<String, String> onepair = Util.splitOnFirst(onerec, "\t");	
			
			alpha.add(Double.valueOf(onepair._1));
			funcs.add(onepair._2);
		}
	}
	
	static class HitPack
	{
		int[][] Nxy = new int[2][2];
		int[] Htot = new int[2];
		int[] Ytot = new int[2];
		int Ftot = 0;
		
		HitPack()
		{
			
		}
		
		// Okay this should just become a Boolean[], and skip
		// the null values
		public HitPack(boolean[] htpred, boolean[] targets)
		{
			Util.massertEq(htpred.length, targets.length);
			
			for(int i = 0; i < htpred.length; i++)
			{
				inc4predTarg(htpred[i], targets[i]);
			}			
		}
		
		public void inc4predTarg(boolean prediction, boolean target)
		{
			int Hval = prediction ? 1 : 0;
			int Yval = target ? 1 : 0;
			
			Nxy[Hval][Yval]++;
			Htot[Hval]++;
			Ytot[Yval]++;
			Ftot++;			
		}
		
		public int getCount(boolean ftpred, boolean target)
		{
			int Hval = ftpred ? 1 : 0;
			int Yval = target ? 1 : 0;
				
			return Nxy[Hval][Yval];
		}		
		
		public double getMiScore()
		{
			double miscore = 0;
			
			for(int xi = 0; xi < 2; xi++)
			{
				for(int yi = 0; yi < 2; yi++)
				{
					if(Nxy[xi][yi] == 0)
						{ continue; }
					
					double pxy = Nxy[xi][yi];
					double px = Htot[xi];
					double py = Ytot[yi];
					
					pxy /= Ftot;
					px /= Ftot;
					py /= Ftot;
					
					miscore+= pxy * Math.log(pxy / (px*py));
				}
			}			
			
			return miscore;
		}
		
		public int getSeedPositive()
		{
			return getCount(true, true); // pred=true, target=true
			
		}
		
		public int getControlPositive()
		{
			return getCount(true, false); // pred=true, target=false	
		}
		
		public int getSeedTotal()
		{
			// target = true, prediction is true or false
			return getCount(true, true) + getCount(false, true);
		}
		
		public int getControlTotal()
		{
			// target = false, prediction is true or false
			return getCount(true, false) + getCount(false, false);
		}		
		
		public double getPercPos()
		{
			double percpos = Nxy[1][1];
			percpos /= Ytot[1];
			return percpos;
		}
		
		public double getPercNeg()
		{
			double percneg = Nxy[1][0];
			percneg /= Ytot[0];
			return percneg;
		}
		
		public int[][] getNxyGrid()
		{
			return Nxy;
		}
		

	}
}
