package com.adnetik.bm_etl;

import java.util.*;
import java.io.*;
import java.text.DecimalFormat;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.bm_etl.BmUtil.*;

import org.apache.hadoop.io.Writable;

/**
 * 
 * Metrics
 * 
 */
public class Metrics implements Writable {
	
	private SortedMap<IntFact, Integer> intmap = Util.treemap();
	private SortedMap<DblFact, Double> dblmap = Util.treemap();
	
	DecimalFormat df = new DecimalFormat("#.######");

	public Metrics() 
	{
		for(IntFact ifact : IntFact.values())
			{ intmap.put(ifact, 0); }	
		
		for(DblFact dfact : DblFact.values())
			{ dblmap.put(dfact, 0.0); }
	}

	public void readFields(DataInput data) throws IOException {
		
		for(IntFact ifact : IntFact.values())
		{
			int val = data.readInt();
			intmap.put(ifact, val);
		}
		
		for(DblFact dfact : DblFact.values())
		{
			double val = data.readDouble();
			dblmap.put(dfact, val);
		}		
	}

	public void write(DataOutput data) throws IOException {
		
		for(IntFact ifact : IntFact.values())
		{
			data.writeInt(intmap.get(ifact));
		}
		
		for(DblFact dfact : DblFact.values())
		{
			data.writeDouble(dblmap.get(dfact));
		}			
	}

	public int getField(IntFact ifact) { return intmap.get(ifact); } 
	public double getField(DblFact dfact) { return dblmap.get(dfact); } 
	
	public void setField(IntFact ifact, int val) { intmap.put(ifact, val); }
	public void setField(DblFact dfact, double val) { dblmap.put(dfact, val); }
	
	@Override
	public String toString() {
		return toString("\t");
	}
	
	public String toString(String delim)
	{
		return toString(delim, true);
	}
	
	public String toQueryStr()
	{
		return toString("&", true);	
	}
	
	public String toString(String delim, boolean inckeys) 
	{
		StringBuffer sb=new StringBuffer();

		for(IntFact ifact : IntFact.values())
		{
			if(inckeys)
			{
				sb.append(ifact.toString());
				sb.append("=");
			}
			
			sb.append(intmap.get(ifact));
			sb.append(delim);
		}
		
		int nproc = 0;
		for(DblFact dfact : DblFact.values())
		{
			if(inckeys)
			{
				sb.append(dfact.toString());
				sb.append("=");
			}
			
			sb.append(df.format(dblmap.get(dfact)));
			
			nproc++;
			if(nproc < DblFact.values().length)
				{ sb.append(delim); }
		}
		
		return sb.toString();
	}	
		
	// Exchnages report cost information in different units,
	// This method standardizes everything
	public void standardizeCostInfo(PathInfo rpath)
	{
		// TODO this is actually not necessary, should be done elsewhere
		// Currency calculations are only relevant for Impression data, which is what we PAY for
		if(rpath.pType != LogType.imp)
			{ return; }
		
		double base_cost = getField(DblFact.cost);
		
		// Some exchanges report cost in different units
		double costdiv = (rpath.pExc == ExcName.rtb || rpath.pExc == ExcName.openx) ? 1000000 : 1000;
		base_cost /= costdiv;	
		
		setField(DblFact.cost, base_cost);		
	}
	
	public void convertCurrencyInfo(CurrCode currency)
	{
		convertCurrencyInfo(currency, true);	
	}
	
	public void convertCurrencyInfo(CurrCode currency, boolean convertBid2Usd)
	{
		CatalogUtil cutil = CatalogUtil.getSing();
		double base_cost = getField(DblFact.cost);
		
		// In most cases, currency will just be USD
		// CurrCode currency = cutil.getCurrencyFromPathInfo(rpath);
		
		// TODO: this information is not actually being used, as far as I know
		setField(DblFact.cost, 	     cutil.convertA2B(currency, CurrCode.USD, base_cost));
		setField(DblFact.cost_euro,  cutil.convertA2B(currency, CurrCode.EUR, base_cost));
		setField(DblFact.cost_pound, cutil.convertA2B(currency, CurrCode.GBP, base_cost));	

		// Make sure everything is in USD
		if(convertBid2Usd)
		{
			double base_bid_amt = getField(DblFact.bid_amount);	
			setField(DblFact.bid_amount, cutil.convertA2B(currency, CurrCode.USD, base_bid_amt));			
		}
	}
	
	public void add(Metrics other) {
		
		for(IntFact ifact : IntFact.values())
		{
			int newval = intmap.get(ifact) + other.intmap.get(ifact);
			intmap.put(ifact, newval);
		}
		
		for(DblFact dfact : DblFact.values())
		{
			double newval = dblmap.get(dfact) + other.dblmap.get(dfact);
			dblmap.put(dfact, newval);
		}			
	}

	public static Metrics fromQueryStr(String qs)
	{
		Map<String, String> pmap = BmUtil.getParseMap(qs);
		Metrics metr = new Metrics();
		
		for(IntFact ifact : IntFact.values())
			{ metr.setField(ifact, Integer.valueOf(pmap.get(ifact.toString()))); }
		
		for(DblFact dfact : DblFact.values())
		{ 
			// TODO: this should be remove-able, it's an artifact of having run a Hadoop job
			// before DblFact.deal_price was added, and then trying to add the data after deal_price was added.
			// Normally the Hadoop job should populate all the Metrics fields, regardless of which 
			// ones are desired by the target DB for upload
			if(!pmap.containsKey(dfact.toString()))
			{
				Util.massert(dfact == DblFact.deal_price, "Unexpected missing metrics field %s", dfact);
				pmap.put(dfact.toString(), "0.0");
			}
			
			metr.setField(dfact, Double.valueOf(pmap.get(dfact.toString())));
		}
		
		return metr;
	}	
	
}