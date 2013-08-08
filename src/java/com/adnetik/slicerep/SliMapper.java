package com.adnetik.slicerep;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.*; // Put all the enums in the same place
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place
 
public class SliMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Metrics> 
{
	PathInfo relPath; 
		
	Map<AggType, SortedSet<DimCode>> dimSetMap;
	
	boolean expandDate = false;
	
	private Long _milliSpent = 0L;
	private Long _numConfigures = 0L;
	
	void initCatalog()
	{
		if(!CatalogUtil.isSingReady())
		{
			try  { 
				// BmUtil.initMaps(job, daycode); 
				_milliSpent = Math.round(CatalogUtil.initSing(DbTarget.internal));
				_numConfigures = 1L;
				
			} catch (Exception ex) {
				// Basically fucked here
				throw new RuntimeException(ex);
			}				
		}
	}
	
	@Override
	public void configure(JobConf job)
	{	
		// Transform from String to DimCodes
		initCatalog();
		
		dimSetMap = CatalogUtil.getSing().getDimSetMap();
		
		expandDate = false;
 	}
 	
	@Override
	public void map( LongWritable key, Text value, OutputCollector<Text, Metrics> output, Reporter rep)
	throws IOException
	{
		if(relPath == null)
		{ 
			try {
				String insplit = rep.getInputSplit().toString();
				if(insplit.indexOf(".lzo") > -1)
				{
					// Lookup from LZO path
					relPath = PathInfo.fromLzoPath(insplit); 
				} else { 
					// Regular method of doing things	
					relPath = new PathInfo(insplit); 
				}
			} catch (Exception ex) { 
				throw new RuntimeException(ex);
			}
			
			// This should be equal to total number of input files, or we have a problem
			rep.incrCounter(BmUtil.Counter.PathChecks, 1);  

			// These will both usually be 0 if JVM reuse is working. 
			// So InitCount will be 10x smaller than PathChecks if JVM reuse is set to 10
			rep.incrCounter(HadoopUtil.Counter.ConfigureTime, _milliSpent);
			rep.incrCounter(HadoopUtil.Counter.InitCount, _numConfigures);
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


		} catch (BidLogFormatException blex) {
			rep.incrCounter(blex.e, 1);
		} catch (Exception ex) {
			ex.printStackTrace();
			rep.incrCounter(HadoopUtil.Counter.GenericError, 1);	
		}
	} 
	
	Pair<DumbDimAgg, Metrics> aggFromLine(String logline) throws BidLogFormatException
	{
		if(relPath == null)
			{ throw new RuntimeException("Must set PathInfo before calling"); }
		
		BleStructure clone = BleStructure.buildStructure(relPath.pType, relPath.pVers, logline);
	
		DumbDimAgg dagg = new DumbDimAgg(clone.getLogEntry());
		Metrics magg = clone.returnMetrics();
		
		magg.standardizeCostInfo(relPath);		
		// dagg.updateCurrencyFromPath(relPath);
		
		return Pair.build(dagg, magg);		
	}
}
