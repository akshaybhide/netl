
package com.adnetik.analytics;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.data_management.*;
import com.adnetik.data_management.ExelateDataMan.*;

public class ExelateBillingReport extends Configured implements Tool
{
	// DoubleBill = number of impressions for which we were double-billed
	public enum ScanData { DoubleBillImps, TotalBillImps, PriceDataLookups };
	
	
	public static void main2(String[] args) throws IOException
	{
		PriceDataPack pdpack = PriceDataPack.getBillingPack("2012-10-01");
		Util.pf("Loaded PD pack with %d entries\n", pdpack.getNumPriceEntries());
		
		Scanner inscan = new Scanner(System.in);
		
		while(inscan.hasNextLine())
		{
			String logline = inscan.nextLine();
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.imp, LogVersion.v19, logline);
			if(ble == null)
				{ continue; }


			for(Pair<String, String> onepair : pdpack.output4ble(ble))
			{
				Util.pf("%s\t%s\n", onepair._1, onepair._2);
				
				// output.collect(new Text(onepair._1), new Text(onepair._2));
			}			
		}		
		
	}
	
	public static void main3(String[] args)
	{
		Scanner inscan = new Scanner(System.in);
		
		while(inscan.hasNextLine())
		{
			String inputline = inscan.nextLine().trim();
			Map<Integer, Long> exsegdata = getExelateSeg(inputline);
			
			//if(inputline.indexOf("ex_") > -1)
			//	{ Util.pf("Found ex_ in inputline\n"); }
			
			if(!exsegdata.isEmpty())
				{ Util.pf("Found ex seg data: %s\n", Util.join(exsegdata.keySet(), "\n")); }
		}
		
	}
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{		
		if(args.length < 1)
		{
			Util.pf("ExelateBillingReport <daycode>\n");
			return 1;
		}
		
		String daycode = args[0];
		daycode = "yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode;
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid day code %s", daycode);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		
		
		PriceDataPack pdpack = PriceDataPack.getBillingPack("2012-10-01");
		Util.pf("Loaded PD pack with %d entries\n", pdpack.getNumPriceEntries());
		
		FileSystem fsys = FileSystem.get(new Configuration());
		{
			String lzopatt = Util.sprintf("/data/imp/*%s*.lzo", daycode);
			List<Path> pathlist = Util.vector();
			// pathlist.addAll(HadoopUtil.getGlobPathList(fsys, lzopatt));
			pathlist.add(new Path("/data/imp/admeld_2012-10-01_v19.lzo"));
			Util.pf("Found %d input paths\n", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
		}
		
		
		job.setStrings("DAY_CODE", daycode);		
		
		// Align Job
		{
			Text a = new Text("");	
			LongWritable b = new LongWritable(1);
 			HadoopUtil.alignJobConf(job, new BillingMapper(), new HadoopUtil.EmptyReducer(), a, a, a, a);	
		}
		
		// Deal with output path
		{
			Path outputpath = new Path(Util.sprintf("/thirdparty/exelate/billing/pricereport/%s", daycode));
			Util.pf("\nTarget Output path is %s", outputpath);
			HadoopUtil.moveToTrash(this, outputpath);
			FileOutputFormat.setOutputPath(job, outputpath);	
		}	
		
		job.setJobName("Exelate Billing Report");
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	

		return 0;
	}
	
	public static String getBillingInfoPath(String daycode)
	{
		return Util.sprintf("/thirdparty/exelate/billing/billinginfo_%s.tsv", daycode);
		
	}

	private static Map<Integer, Long> getExelateSeg(String seginfo)
	{
		Map<Integer, Long> segmap = Util.treemap();
					
		for(String oneseg : seginfo.split("\\|"))
		{			
			if(!oneseg.startsWith("ex_"))
				{ continue; }
			
			// Util.pf("Segment is %s\n", oneseg);
			
			String[] id_ts = oneseg.substring(3).split(":");
			
			segmap.put(Integer.valueOf(id_ts[0]), Long.valueOf(id_ts[1]));
		}
		
		return segmap;
	}	
	
	
	public static class BillingMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		PathInfo _pInfo = null;
		PriceDataPack _pricePack = null;
		
		@Override
		public void map( LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter rep)
		throws IOException
		{
			if(_pInfo == null)
			{
				String path = rep.getInputSplit().toString();
				_pInfo = PathInfo.fromLzoPath(path);				
			}
			
			if(_pricePack == null)
			{
				_pricePack = PriceDataPack.getBillingPack("2012-10-01");
				rep.incrCounter(ScanData.PriceDataLookups, 1);
			}
			
			BidLogEntry ble = BidLogEntry.getOrNull(_pInfo.pType, _pInfo.pVers, value.toString());
			if(ble == null)
				{ return; }
			
			try {
				for(Pair<String, String> onepair : _pricePack.output4ble(ble))
				{
					output.collect(new Text(onepair._1), new Text(onepair._2));
				}
			} catch (Exception ex) {
				
					
			}
			
			// output.collect(new Text(combkey), HadoopUtil.TEXT_ONE);
		} 
	}	
	
	public static class PriceDataPack
	{
		private Map<Integer, Map<Integer, Map<Integer, Double>>> _bigPriceMap = Util.treemap();		

		private int _numPriceEntries = 0;
		
		public static PriceDataPack getBillingPack(String daycode) throws IOException
		{
			FileSystem fsys = FileSystem.get(new Configuration());
			Scanner sc = HadoopUtil.hdfsScanner(fsys, getBillingInfoPath(daycode));
			PriceDataPack pdpack = new PriceDataPack();
			
			while(sc.hasNextInt())
			{
				int lineid = sc.nextInt();
				int utw = sc.nextInt();
				int segid = sc.nextInt();
				double price = sc.nextDouble();
				pdpack.addPriceInfo(lineid, utw, segid, price);
			}
			
			return pdpack;
		}
		
		private void addPriceInfo(int lineid, int utw, int segid, double price)
		{
			Util.setdefault(_bigPriceMap, lineid, new TreeMap<Integer, Map<Integer, Double>>());
			Util.setdefault(_bigPriceMap.get(lineid), utw, new TreeMap<Integer, Double>());
			_bigPriceMap.get(lineid).get(utw).put(segid, price);
			_numPriceEntries++;
		}
		
		public boolean hasMap(Integer lineid, Integer utw)
		{
			return _bigPriceMap.containsKey(lineid) && _bigPriceMap.get(lineid).containsKey(utw);	
		}
		
		public Map<Integer, Double> getPriceMap(Integer lineid, Integer utw)
		{
			Util.massert(hasMap(lineid, utw), "Must check for map existence before calling");
			return _bigPriceMap.get(lineid).get(utw);
		}
		
		public int getNumPriceEntries()
		{
			return _numPriceEntries;	
		}
		
		public List<Pair<String, String>> output4ble(BidLogEntry ble)
		{
			List<Pair<String, String>> output = Util.vector();
			
			Integer lineid = ble.getIntField("line_item_id");
			Integer utw = ble.getIntField("utw");
			
			if(!hasMap(lineid, utw))
				{ return output; }
			
			Map<Integer, Double> pricemap = getPriceMap(lineid, utw);
			Set<Integer> segset = getExelateSeg(ble.getField("segment_info")).keySet();
			
			for(Integer oneseg : segset)
			{
				if(pricemap.containsKey(oneseg))
				{
					String combval = Util.sprintf("%d\t%d\t%.03f", lineid, utw, pricemap.get(oneseg));
					output.add(Pair.build(""+oneseg, combval));
				}
			}	

			return output;
		}
	}
	
	/*
	public static class LookupReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		private String _dayCode;		
		
		@Override
		public void configure(JobConf job)
		{	
			try {
				_dayCode = job.get("DAY_CODE");
				ExelateDataMan.setSingQ(_dayCode);
								
			} catch (IOException ioex) {
				throw new RuntimeException(ioex);	
			}
		}
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{
			String[] wtp_camp_line = key.toString().split(Util.DUMB_SEP);
						
			String seginfo = "NOTFOUND";
			
			ExUserPack expack = ExelateDataMan.getSingQ().lookup(wtp_camp_line[0]);	
			if(expack != null)
			{
				seginfo = Util.join(expack.getAllSegData(), ",");
				reporter.incrCounter(ScanData.FoundExUsers, 1);
			}
			
			collector.collect(key, new Text(seginfo));
		}		
	}	
	*/	
	
	
}

