
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
import com.adnetik.shared.DbUtil.InfSpooler;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.data_management.*;
import com.adnetik.data_management.ExelateDataMan.*;

import com.adnetik.analytics.ThirdPartyDataUploader.*;

// Second pass at the ExelateBilling report
public class ExelateBillingReport
{
	String _dayCode;
	
	Map<Integer, Double> _costCpmMap = Util.treemap();
	
	// Map<Integer, Pair<Part3Code, Integer>> _lineSegMap = Util.treemap();
	
	
	// private AdboardSegInfoHelper _segInfoHelper = new AdboardSegInfoHelper();
	private CostLookupStub _costLookup = new CostLookupStub();
	
	private InfSpooler _infSpooler;
	
	private SimpleMail _logMail;
	
	private Map<ExcName, Integer> _excHitMap = Util.treemap();
	private int _foundDataHits = 0;
	private int _noDataHits = 0;
	private int _excLineHits = 0;
	
	
	public enum MainField { line_itemInt, max_seg_idInt, price_cpmDbl, uuidStr, nfsdateDate };
	
	private BufferedWriter _subWriter;
	
	private static String BILLING_SUB_PATH = "/local/fellowship/thirdparty/exelate/billing";
	
	private static String INF_PATH = BILLING_SUB_PATH + "/exelate_inf.tsv";
	
	public static void main(String[] args) throws IOException
	{
		if(args.length < 1)
		{
			Util.pf("ExelateBillingReport <daycode>\n");
			return;
		}
		
		// TODO: make sure Master list file is present
		String dc = args[0];
		dc = "yest".equals(dc) ? TimeUtil.getYesterdayCode() : dc;
		dc = "yestyest".equals(dc) ? TimeUtil.dayBefore(TimeUtil.getYesterdayCode()) : dc;
		Util.massert(TimeUtil.checkDayCode(dc), "Invalid day code %s", dc);			
		
		ArgMap optmap = Util.getClArgMap(args);
		int maxfile = optmap.getInt("maxfile", Integer.MAX_VALUE);
		
		ExelateBillingReport exbill = new ExelateBillingReport(dc);
		exbill.fullOperation(maxfile);
	}
	
	public ExelateBillingReport(String dc) throws IOException
	{
		_dayCode = dc;
		
		_infSpooler = new InfSpooler(MainField.values(), INF_PATH,
			new Party3Db(), "ex_usage_main");
		
		_infSpooler.setBatchSize(10000);
		
		_logMail = new SimpleMail("ExelateBillingReport " + _dayCode);
		_infSpooler.setLogMail(_logMail);
		
		
		popCostMap();
		
		_costLookup.initFromAdBoard();
		if(!_costLookup.checkHavePriceInfo(_costCpmMap, _logMail))
		{
			// _logMail.send2admin();
			// Util.massert(false, "Cannot proceed without value price info, aborting");
		}
		
		// The point here is to fail fast
		ExelateDataMan.setSingQ(_dayCode);		
		// _segInfoHelper.initData();
	}
	
	public void fullOperation(int maxfile) throws IOException
	{
		deleteOld();
		
		sendImp2Sub(maxfile);
		
		sideBySideScan();
		
		_infSpooler.finish();
		_logMail.send2admin();
		
		File subfile = new File(getLocalSubPath());
		subfile.delete();
	}
	
	private void deleteOld()
	{
		String delsql = Util.sprintf("DELETE FROM ex_usage_main WHERE nfsdate = '%s'", _dayCode);
		int delrows = DbUtil.execSqlUpdate(delsql, new Party3Db());
		_logMail.pf("Deleted %d rows of old data\n", delrows);
	}
	
	void sendImp2Sub(int maxfile) throws IOException
	{
		List<String> targlist = getTargPathList();
		Collections.shuffle(targlist);
		
		double startup = Util.curtime();
		_logMail.pf("Starting scan of IMP data, found %d paths\n", targlist.size());
		
		_subWriter = FileUtils.getWriter(getLocalSubPath());
		
		for(int pid : Util.range(targlist.size()))
		{
			String onepath = targlist.get(pid);
			
			try { send2sub(onepath); }
			catch (IOException ioex) {
				
				_logMail.pf("IOException reading file %s\n", onepath);
				_logMail.addExceptionData(ioex);
			}
			
			if((pid % 10) == 0)
			{
				double timesecs = (Util.curtime() - startup)/1000;
				double avgtime = timesecs/(pid+1);
				_logMail.pf("Completed path %d/%d, exelate line hits %d, average time=%.03f, estcomplete=%s\n", 
					pid, targlist.size(), _excLineHits, avgtime,
					TimeUtil.getEstCompletedTime(pid+1, targlist.size(), startup));
			}				
			
			if(pid >= maxfile)
				{ break; }
		}
		
		_subWriter.close();
		
	}
	
	// Ugh, why is this not a one-liner
	private List<String> getTargPathList()
	{
		List<String> pathlist = Util.vector();
		
		for(ExcName exc : ExcName.values())
		{
			List<String> plist = Util.getNfsLogPaths(exc, LogType.imp, _dayCode);
			if(plist != null)
				{ pathlist.addAll(plist); }
		}
		
		return pathlist;
	}
	
	private String getLocalSubPath()
	{
		return Util.sprintf("%s/sub_%s.txt", BILLING_SUB_PATH, _dayCode);
	}
	
	private void send2sub(String onefile) throws IOException
	{
		PathInfo pinfo = new PathInfo(onefile);
		BufferedReader bread = FileUtils.getGzipReader(onefile);
		int lcount = 0;
				
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			lcount++;
			
			BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
			if(ble == null)
				{ continue; }
			
			int lineid = ble.getIntField(LogField.line_item_id);
			if(!_costLookup.isExTargetLine(lineid))
				{ continue; }		
			
			_excLineHits++;
			
			// Discard improperly formed WTP_IDS
			WtpId wid = WtpId.getOrNull(ble.getField(LogField.wtp_user_id));
			if(wid == null)
				{ continue; }
						
			// Write <user_id, line_id> pairs to sub file
			_subWriter.write(wid.toString());
			_subWriter.write("\t");
			_subWriter.write(""+lineid);
			_subWriter.write("\n");
		}
		
		bread.close();
		
		// _logMail.pf("Finished processing file, found %d lines\n", lcount);		
	}
	
	// Scan the Exelate Queue and the sub queue in parallel
	
	private void sideBySideScan() throws IOException
	{

		// true=do the sort for me
		SortedFileMap sfmap = SortedFileMap.buildFromFile(new File(getLocalSubPath()), true);
		_logMail.pf("Finished sorting SF map\n");
		
		int curprefint = 0;
		double startup = Util.curtime();
		
		while(sfmap.hasNext())
		{
			Map.Entry<String, List<String>> anentry = sfmap.pollFirstEntry();
			String wtpid = anentry.getKey();
			
			int prefint = Integer.parseInt(wtpid.substring(0, 2), 16);
			
			if(prefint != curprefint)
			{
				_logMail.pf("Done with users with prefix %d, found-data hits %d, no-data hits %d, estcomplete %s\n", 
					curprefint, _foundDataHits, _noDataHits, TimeUtil.getEstCompletedTime(curprefint+1, 256, startup));
				curprefint = prefint;
			}
			
			// Util.pf("Found %d hits for userid=%s\n", anentry.getValue().size(), anentry.getKey());
			
			ExUserPack expack = ExelateDataMan.getSingQ().lookup(wtpid);
			
			if(expack == null)
			{
				// Here we have no Exelate data for the user at all
				// Util.pf("Warning, no exelate data found for userid %s\n", wtpid);
				_noDataHits += anentry.getValue().size();
				
			} else {
				
				Set<Integer> userset = expack.getAllSegData();
				
				for(String oneline : anentry.getValue())
				{
					int lineid = Integer.valueOf(oneline.split("\t")[1]);
					Set<Integer> linetargset = _costLookup.getExTargSet4Line(lineid);
					
					TreeSet<Pair<Double, Integer>> cost2seg = Util.treeset();
					for(int onetarg : linetargset)
					{
						if(userset.contains(onetarg))
						{
							Util.massert(_costCpmMap.containsKey(onetarg),
								"No cost information found for segment %d", onetarg);
							
							double cost = _costCpmMap.get(onetarg);
							cost2seg.add(Pair.build(cost, onetarg));
						}
					}
					
					// Util.pf("Found userset size=%d, targset size=%d, overlap size=%d\n",
					//	userset.size(), linetargset.size(), cost2seg.size());
					
					if(!cost2seg.isEmpty())
					{
						double maxprice = cost2seg.last()._1;
						int maxseg = cost2seg.last()._2;
						
						// Util.pf("Found maxprice=%.03f for lineid=%d, maxseg=%d, wtp=%s\n",
						//	maxprice, lineid, maxseg, wtpid);
						
						sendRow(lineid, maxprice, maxseg, wtpid);
						
						// Util.incHitMap(_excHitMap, pinfo.pExc);
						_foundDataHits++;
						
					} else {
						
						// Here the user has SOME Exelate data, just not the segments we think
						// are associated with the actual line item.
						
						// Util.pf("Warning: impression served for lineid=%d to userid=%s, user does not seem to be in pool\n",
						// 		lineid, wtpid);
						
						_noDataHits++;
					}
				}
				
				// Util.pf("Found %d segments for userid=%s\n", expack.getAllSegData().size(), wtpid);	
				
			}
		}
	}
	
	private void sendRow(Integer lineid, Double maxprice, Integer maxseg, String wtpuser) throws IOException
	{
		_infSpooler.setInt(MainField.line_itemInt, lineid);
		_infSpooler.setDbl(MainField.price_cpmDbl, maxprice);
		_infSpooler.setInt(MainField.max_seg_idInt, maxseg);
		_infSpooler.setStr(MainField.uuidStr, wtpuser);
		
		_infSpooler.setDate(MainField.nfsdateDate, _dayCode);
		_infSpooler.flushRow();
	}
	
	private static String getLastPriceDate()
	{
		String sql = "SELECT max(daycode) FROM ex_price_info";
		List<java.sql.Date> plist = DbUtil.execSqlQuery(sql, new Party3Db());
		return plist.get(0).toString();
	}

	private void popCostMap()
	{
		String lastdate = getLastPriceDate();
		String popsql = Util.sprintf("SELECT seg_id, price_cpm FROM ex_price_info WHERE daycode='%s'", lastdate);
		DbUtil.popMapFromQuery(_costCpmMap, popsql, new Party3Db());
		Util.pf("Built Cost2Cpm map, found %d entries\n", _costCpmMap.size());
	}		
	
	public static abstract class ExCostInfoTool 
	{
		public boolean isExTargetLine(int lineid)
		{
			Set<Integer> targset = getExTargSet4Line(lineid);
			return (targset != null && !targset.isEmpty());
		}
		
		public abstract Set<Integer> getExTargSet4Line(int lineid);
	}
	
	
	public static class CostLookupStub extends ExCostInfoTool
	{
		Map<Integer, Set<Integer>> _line2SegMap = null;		
		
		public CostLookupStub()
		{
			
			
		}
		
		void initFromAdBoard()
		{
			AdboardSegInfoHelper helper = new AdboardSegInfoHelper();
			helper.initData();
			_line2SegMap = helper.getLid2SegSetMap(Part3Code.EX);			
		}
		
		public Set<Integer> getExTargSet4Line(int lineid)
		{
			Util.massert(_line2SegMap != null,
				"Lookup code called before initializing CostLookup object");
			
			return _line2SegMap.get(lineid);
		}
		
		
		// Exelate ID to Set of LineItems
		private Map<Integer, Set<Integer>> getRevMap()
		{
			Map<Integer, Set<Integer>> revmap = Util.treemap();
			
			for(Integer lineid : _line2SegMap.keySet())
			{ 
				for(Integer segid : _line2SegMap.get(lineid))
				{
					Util.setdefault(revmap, segid, new TreeSet<Integer>());	
					revmap.get(segid).add(lineid);
				}
			}
			
			return revmap;			
		}
		
		public boolean checkHavePriceInfo(Map<Integer, Double> seg2pricemap, SimpleMail logmail)
		{
			boolean priceokay = true;
			
			Map<Integer, Set<Integer>> revmap = getRevMap();
			
			for(int segid : revmap.keySet())
			{
				if(!seg2pricemap.containsKey(segid))
				{
					logmail.pf("Missing price information for Exelate segment %d, connected to line items %s\n",
						segid, revmap.get(segid));	
					
					priceokay = false;
					
					// Okay, this is kind of a hack, but what the hell
					seg2pricemap.put(segid, 0D);
				}
			}
			
			return priceokay;
		}
	}
	
	
	public static class AdboardSegInfoHelper
	{
		private Map<Integer, Set<Integer>> _line2TargSetMap = Util.treemap();
		
		private Map<Integer, Pair<Part3Code, Integer>> _targId2SegMap = Util.treemap();
		
		
		// Line Item --> Campaign ID
		private Map<Integer, Integer> _lid2campMap = Util.treemap();

		public void initData()
		{
			popLine2CampMap();
			popLineTargListMap();	
			popTarg2SegMap();
		}
		
		public int getCampaign4Lid(int lid)
		{
			Util.massert(!_lid2campMap.isEmpty(), "Must initialize lid2campMap first");
			return _lid2campMap.get(lid);
		}
				
		
		// Return LineItemId --> Set<SegmentID> for a given Part3Code
		public Map<Integer, Set<Integer>> getLid2SegSetMap(Part3Code p3c)
		{
			Util.massert(!_line2TargSetMap.isEmpty(), "Need to initialize line2targSetMap first");
			Util.massert(!_targId2SegMap.isEmpty(), "Need to initialize targId2SegMap first");
			
			Map<Integer, Set<Integer>> resmap = Util.treemap();
			
			for(Integer lid : _line2TargSetMap.keySet())
			{
				for(Integer targid : _line2TargSetMap.get(lid))
				{
					Pair<Part3Code, Integer> segdata = _targId2SegMap.get(targid);
					
					if(segdata == null)
					{
						// Util.pf("Could not find P3C/SEG code for targid=%d\n", targid); 	
						continue;
					}
					
					if(segdata._1 !=  p3c)
					{
						// Util.pf("Found non-target  pair %s, target is %s\n", segdata, p3c);
						continue;
					}
					
					Util.setdefault(resmap, lid, new TreeSet<Integer>());
					resmap.get(lid).add(segdata._2);
				}
			}
			
			return resmap;
		}
		
		private void popLine2CampMap()
		{
			String targsql = "SELECT id, campaign_id FROM adnetik.line_item";
			Map<java.math.BigInteger, java.math.BigInteger> idmap = Util.treemap();
			DbUtil.popMapFromQuery(idmap, targsql, new DbUtil.ThorinExternalConn());
			
			for(Map.Entry<java.math.BigInteger, java.math.BigInteger> oneentry : idmap.entrySet())
			{
				//Util.pf("LID=%d maps to campaign %d\n", oneentry.getKey(), oneentry.getValue());
				_lid2campMap.put(oneentry.getKey().intValue(), oneentry.getValue().intValue());
			}			
		}
		

		private void popLineTargListMap()
		{
			String targsql = "SELECT id, advanced_formula FROM adnetik.line_item WHERE advanced_formula IS NOT NULL";
			Map<java.math.BigInteger, String> formmap = Util.treemap();
			DbUtil.popMapFromQuery(formmap, targsql, new DbUtil.ThorinExternalConn());
			
			for(Map.Entry<java.math.BigInteger, String> oneentry : formmap.entrySet())
			{
				//Util.pf("Converting formula %s\n", oneentry.getValue());
				
				try {
					Set<Integer> segset = advForm2Set(oneentry.getValue());
					_line2TargSetMap.put(oneentry.getKey().intValue(), segset);
				} catch (NumberFormatException nfex) {
					Util.pf("Error converting formula %s, skipping\n", oneentry.getValue());	
				}
			}
			
			Util.pf("Finished populating line2targ, size is %d\n", _line2TargSetMap.size());
		}		
		
	
		
		
		private void popTarg2SegMap()
		{
			for(Part3Code p3c : Part3Code.values())
			{
				String targsql = "SELECT id, external_id FROM adnetik.targeting_list WHERE external_id LIKE '" + p3c.toString().toLowerCase() + "_%'";
				Util.pf("TARG SQL is %s\n", targsql);
				Map<java.math.BigInteger, String> formmap = Util.treemap();
				DbUtil.popMapFromQuery(formmap, targsql, new DbUtil.ThorinExternalConn());
				
				String p3cpref = Util.sprintf("%s_", p3c.toString().toLowerCase());
				
				for(Map.Entry<java.math.BigInteger, String> oneentry : formmap.entrySet())
				{
					String[] toks = oneentry.getValue().split("_");
					
					if(toks.length == 2 && toks[0].toLowerCase().equals(p3c.toString().toLowerCase()))
					{
						// Util.pf("Found id=%d, external_id = %s for p3c %s\n", oneentry.getKey(), oneentry.getValue(), p3c);
						Integer segid = Integer.valueOf(toks[1]);
						_targId2SegMap.put(oneentry.getKey().intValue(), Pair.build(p3c, segid));
					}
				}			
			}
		}
		
		private static Set<Integer> advForm2Set(String advform)
		{
			int startpos = 0;
			Set<Integer> formset = Util.treeset();
			
			while(advform.indexOf('[', startpos) > -1)
			{
				int s = advform.indexOf('[', startpos);
				int e = advform.indexOf(']', s);
				
				Util.massert(e != -1, "Parse error in formula %s\n", advform);
				
				String segidstr = advform.substring(s+1, e);
				if(segidstr.endsWith("*"))
					{ segidstr = segidstr.substring(0, segidstr.length()-1); }
				
				formset.add(Integer.valueOf(segidstr));
				// Util.putNoDup(formset, Integer.valueOf(segidstr));
				// Util.pf("Seg id str is %s\n", segidstr);
				
				startpos = e;
			}
			
			return formset;
		}		
	}
}


