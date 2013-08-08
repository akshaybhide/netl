
package com.adnetik.analytics;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;

public class HistogramTool<T extends Number>
{
	TreeMap<T, String> rghtEdge2Labl = new TreeMap<T, String>();
	TreeMap<T, Double> rghtEdge2Mass = new TreeMap<T, Double>(); // edge->Masss
	
	
	private double _totalMass;
	
	public static void main(String[] args) throws IOException
	{
		if(args.length < 2)
		{
			Util.pf("\nUsage: HistogramTool <datafile> <edgefile>");	
			System.exit(1);
		}
		
		/*
		HistogramTool htool = new HistogramTool();
		htool.readBucketFile(args[1]);
		//htool.printBucketInfo();
		
		htool.readDataFile(args[0]);
		htool.printResults();
		*/
	}
	
	public HistogramTool(List<T> binedges)
	{
		for(T boundary : binedges)
		{
			rghtEdge2Mass.put(boundary, 0.0D);
		}
	}
	
	
	public boolean withinRange(T x)
	{
		return getRange(x) != null;
	}
	
	public Pair<T, T> getRange(T x)
	{		
		T low = rghtEdge2Mass.lowerKey(x);
		T hgh = rghtEdge2Mass.ceilingKey(x);
		
		if(low == null || hgh == null)
			{ return null; }
		
		return Pair.build(low, hgh);
	}	
	
	public void incrementValue(T xvalue, double weight)
	{
		Util.massert(withinRange(xvalue), "Attempt to add out-of-range value: %.03f", xvalue);
		
		T lookup = getRange(xvalue)._2;		
		Double prev_val = rghtEdge2Mass.get(lookup);
		rghtEdge2Mass.put(lookup, prev_val+weight);
		
		_totalMass += weight;
	}

	
	public void incrementValue(T xvalue)
	{
		incrementValue(xvalue, 1.0D);	
	}
	
	public List<Pair<Number, Double>> getBinWeightList()
	{
		List<Pair<Number, Double>> binlist = Util.vector();
		
		for(Number binedge : rghtEdge2Mass.keySet())
			{ binlist.add(Pair.build(binedge, rghtEdge2Mass.get(binedge))); }

		return binlist;
	}
	
	public double getTotalMass()
	{
		return _totalMass;	
	}

	/*
	void readBucketFile(String path) throws IOException
	{
		Scanner sc = new Scanner(new File(path));
		
		while(sc.hasNextLine())
		{
			String line = sc.nextLine().trim();
			
			if(line.length() == 0)
				{ continue; }
			
			String[] toks = line.split(",");
			Double binedge = Double.valueOf(toks[0]);
			String binlabl = Util.sprintf("- %s", binedge);
			
			if(toks.length > 1)
				{ binlabl = toks[1]; }
			
			Util.massert(edgeLabelMap.isEmpty() || binedge > edgeLabelMap.lastKey(), "Edges must be presented in order.");
			
			edgeLabelMap.put(binedge, binlabl);
			
			// Initialize mass map
			binMassMap.put(binedge, 0.0D);
		}
		
	}
	*/
	
	/*
	void printBucketInfo()
	{
		for(Double edge : edgeLabelMap.keySet())
		{
			Util.pf("\nEdge=%.03f, Label=%s", edge, edgeLabelMap.get(edge));
		}
	}
	*/
	

	
	/*
	void readDataFile(String path) throws IOException
	{
		Util.massert(!rghtEdge2Mass.isEmpty(), "Must initialize the edge data first");
		
		Scanner sc = new Scanner(new File(path));
		
		while(sc.hasNextLine())
		{
			String line = sc.nextLine().trim();
			
			if(line.length() == 0)
				{ continue; }
			
			String[] toks = line.split(",");
			Double xvalue = Double.valueOf(toks[0]);
			Double yvalue = 1.0;
			
			if(toks.length > 1)
				{ yvalue = Double.valueOf(toks[1]); }
			
			incrementValue(xvalue, yvalue);
		}	
		
		sc.close();
	}
	*/

	/*
	void printResults()
	{
		for(Double edge : edgeLabelMap.keySet())
		{
			Util.pf("%s,%.03f\n", edgeLabelMap.get(edge), binMassMap.get(edge));
		}
	}
	*/
}
