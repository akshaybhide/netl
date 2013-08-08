package com.adnetik.bm_etl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place
 
public class MapTransformer extends MapReduceBase implements Mapper<LongWritable, Text, Text, Metrics> 
{
	PathInfo relPath; 
		
	Map<AggType, SortedSet<DimCode>> dimSetMap;
	
	boolean expandDate = false;
	
	Long milliSpent = null;
	
	void initCatalog(String daycode)
	{
		try  { 
			// BmUtil.initMaps(job, daycode); 
			milliSpent = Math.round(CatalogUtil.initSing(daycode, DbTarget.external));
		
		} catch (Exception ex) {
			// Basically fucked here
			throw new RuntimeException(ex);
		}		
	}
	
	@Override
	public void configure(JobConf job)
	{	
		// Transform from String to DimCodes
		initCatalog(job.get("DAY_CODE"));
		
		dimSetMap = CatalogUtil.getSing().getDimSetMap();
		
		expandDate = "true".equals(job.get("EXPAND_DATE"));
 	}
 	
	@Override
	public void map( LongWritable key, Text value, OutputCollector<Text, Metrics> output, Reporter rep)
	throws IOException
	{
		if(relPath == null)
		{ 
			try { relPath = new PathInfo(rep.getInputSplit().toString()); }
			catch (Exception ex)  { throw new RuntimeException(ex); }
			
			// This should be equal to total number of input files, or we have a problem
			rep.incrCounter(BmUtil.Counter.PathChecks, 1);  
		}
		
		// It's stupid that this needs to be done here, the issue is 
		// we don't have a reporter to talk to in configure
		if(milliSpent != null)
		{
			rep.incrCounter(HadoopUtil.Counter.ConfigureTime, milliSpent);
			milliSpent = null;
		}
		
		try {
			Pair<DumbDimAgg, Metrics> aggpair = aggFromLine(value.toString());
			
			for(AggType onetype : dimSetMap.keySet())
			{
				String aggkey = aggpair._1.computeKey(dimSetMap.get(onetype), expandDate);
				{
					StringBuffer sb = new StringBuffer();				
					sb.append("aggtype=");
					sb.append(onetype.toString());
					sb.append("&");
					sb.append(aggkey);
					output.collect(new Text(sb.toString()), aggpair._2);				
				}

				// Dumb Check - this will throw an exception if something funky is happening
				{ Map<String, String> pmap = BmUtil.getParseMap(aggkey); }
			}
			
			// Report records that have no currency
			if(aggpair._1.getCurrency() == null)
				{ rep.incrCounter(HadoopUtil.Counter.NoCurrencyRecords, 1); }


		} catch (BidLogFormatException blex) {
			rep.incrCounter(blex.e, 1);
		} catch (Exception ex) {
			// ex.printStackTrace();
			// throw new RuntimeException(ex);
			rep.incrCounter(HadoopUtil.Counter.GenericError, 1);	
		}
	} 
	
	Pair<DumbDimAgg, Metrics> aggFromLine(String logline) throws BidLogFormatException
	{
		if(relPath == null)
			{ throw new RuntimeException("Must set PathInfo before calling"); }
		
		BleStructure clone = BleStructure.buildStructure(relPath.pType, relPath.pVers, logline);
		DumbDimAgg dumbagg = new DumbDimAgg(clone.logentry);
		Metrics magg = clone.returnMetrics();
		magg.standardizeCostInfo(relPath);
		
		// Only imps have cost, so only imps need to 
		if(relPath.pType != LogType.imp)
		{ 
			CurrCode ccode = dumbagg.getCurrency();
			ccode = (ccode == null ? CurrCode.USD : ccode);
			magg.convertCurrencyInfo(ccode);
		}

		return Pair.build(dumbagg, magg);		
	}
}
