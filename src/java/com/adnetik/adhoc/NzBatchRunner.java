
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.slicerep.*;

// Continuous mode version of NzUpload
public class NzBatchRunner
{
	private static final String NZBR_GIMP_PATH = "/home/burfoot/netezza/NZBR_GIMP.txt";
	
	private ControlTable _cleanList = new ControlTable.CleanListImpl("/home/burfoot/netezza/cleandir");
	
	private BufferedWriter _curWriter; 
	private int _curBatchCount = 0;
	
	public static void main(String[] args) throws Exception
	{
		
		NzBatchRunner nzbr = new NzBatchRunner();
		nzbr.startEtl();
	}
	
	private void startEtl() throws Exception
	{
		while(true)
		{
			SortedSet<String> nextbatch = _cleanList.nextBatch(1, 500);
			
			SortedMap<String, SortedSet<String>> breakmap = BatchRunner.sortByNfsDate(nextbatch);
			String relday = breakmap.firstKey();
			SortedSet<String> onedayset = breakmap.get(relday);

			Util.pf("Processing batch, size is %d, first/last is \n%s\n%s\n",
				onedayset.size(), getSimpleName(onedayset.first()), getSimpleName(onedayset.last()));
			
			processBatch(onedayset, relday);
			
			_cleanList.reportFinished(onedayset);
			
			Util.pf("Done with batch, sleeping... ");
			Thread.sleep(15*60*1000);
		}
		
	}
	
	private void processBatch(SortedSet<String> onebatch, String relday) throws IOException
	{
		for(String nfslogpath : onebatch)
		{
			subFileProcess(nfslogpath, relday);
		}
		
		// Last flushdata, do we really need to do it...?
		flushData();
	}
	
	private void subFileProcess(String nfslogpath, String daycode) throws IOException
	{
		Random jrand = new Random();
		BufferedReader bread = FileUtils.getGzipReader(nfslogpath);
		PathInfo pinfo = new PathInfo(nfslogpath);
		
		int onefilecount = 0;
		
		// Probability we KEEP the record
		double keep_prob = (pinfo.pType == LogType.bid_all ? .1 : 1);
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			
			BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
			
			// Util.pf("Campaign ID is %d, wtp id is %s\n", 
			// 	ble.getIntField(LogField.campaign_id), ble.getField(LogField.wtp_user_id));
			
			if(jrand.nextDouble() > keep_prob)
				{ continue; }
			
			List<String> onerow = Util.vector();
			onerow.add(ble.getField(LogField.date_time));
			onerow.add(daycode);
			
			add2row(onerow, ble, LogField.wtp_user_id, 40);
			add2row(onerow, ble, LogField.transaction_id, 40);
			add2row(onerow, ble, LogField.ad_exchange, 15);
			add2row(onerow, ble, LogField.campaign_id, 10);
			add2row(onerow, ble, LogField.line_item_id, 20);
			add2row(onerow, ble, LogField.size, 10);
			add2row(onerow, ble, LogField.utw, 10);
			add2row(onerow, ble, LogField.currency, 3);
			add2row(onerow, ble, LogField.language, 20);
			add2row(onerow, ble, LogField.domain, 100);
			add2row(onerow, ble, LogField.user_country, 2);
			add2row(onerow, ble, LogField.user_region, 2);
			add2row(onerow, ble, LogField.user_city, 100);
			add2row(onerow, ble, LogField.user_postal, 15);
			add2row(onerow, ble, LogField.bid, 20);
			
			{
				String winprice = (pinfo.pType == LogType.imp ? "0.0" : Util.sprintf("%.04f", Util.getWinnerPriceCpm(ble)));
				//onerow.add(Util.sprintf("%.04f", Util.getWinnerPriceCpm(ble)));
				onerow.add(winprice);
			}
			
			onerow.add(jrand.nextInt(1000)+"");				
			onerow.add(getLogTypeCode(pinfo.pType));

			// String row = Util.join(onerow, "\t");
			// String row = Util.sprintf("%d\t%s", ble.getIntField(LogField.campaign_id), ble.getField(LogField.wtp_user_id));
			///rowlist.add(row);
			
			// Open the writer object
			openIfNecessary();			
			FileUtils.writeRow(_curWriter, "\t", onerow.toArray());
			_curBatchCount++;
			onefilecount++;
			
			if(_curBatchCount > 100000)
				{ flushData(); }
		}
		
		bread.close();			
	}
	
	private static Map<LogType, String> _LOGTYPE_2_CODE = Util.treemap();
	
	static {
		
		_LOGTYPE_2_CODE.put(LogType.imp, "IMP");
		_LOGTYPE_2_CODE.put(LogType.bid_all, "BID");
		_LOGTYPE_2_CODE.put(LogType.click, "CLK");
		_LOGTYPE_2_CODE.put(LogType.conversion, "CNV");
	}
	
	private String getSimpleName(String fullnfspath)
	{
		return  (new File(fullnfspath)).getName();
	}
	
	private String getLogTypeCode(LogType lt)
	{	
		String s = _LOGTYPE_2_CODE.get(lt);
		Util.massert(s != null, "LogType %s not found in map", lt);
		return s;
	}
	
	static void add2row(List<String> onerow, BidLogEntry ble, LogField lf, int maxsize)
	{
		String fval = ble.hasField(lf) ? ble.getField(lf) : "";
		fval = (fval.length() > maxsize ? fval.substring(0, maxsize) : fval);
		onerow.add(fval);
	}	
	
	
	private void openIfNecessary() throws IOException
	{
		if(_curWriter == null)
		{
			_curWriter = FileUtils.getWriter(NZBR_GIMP_PATH);	
			_curBatchCount = 0;			
		}
	}
	
	private void flushData() throws IOException
	{
		if(_curWriter != null)
		{
			_curWriter.close();	
			_curWriter = null;
		}
		
		Util.pf("Transfering file, batchcount is %d\n", _curBatchCount);
		
		TestNzUpload.transferFile(new File(NZBR_GIMP_PATH), new SimpleMail("gimp"));
		TestNzUpload.remoteUploadCommand(NZBR_GIMP_PATH, "imp_test_1", new SimpleMail("gimp"));
		
		_curBatchCount = 0;		
	}
}
