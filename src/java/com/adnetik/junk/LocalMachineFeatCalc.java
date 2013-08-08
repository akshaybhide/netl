
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import java.text.SimpleDateFormat;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.ExcName;

import com.adnetik.shared.*;
import com.adnetik.data_management.BluekaiDataMan;

// TODO: is this file at all necessary?
public class LocalMachineFeatCalc
{	
	public enum LocCommLine { dograb , featcalc }
	
	public static void main(String[] args) throws Exception
	{	
		LocCommLine lcl;
		
		try { lcl = LocCommLine.valueOf(args[0]);  }
		catch (Exception ex ) {
			
			Util.pf("Usage: LocalMachineFeatCalc <dograb|featcalc> daycode poslist");
			return;
		}
		
		String endday = args[1];
		String poscode = args[2];
		String negcode = ListInfoManager.getSing().getNegListCode(poscode);
		
		if(lcl == LocCommLine.dograb)
		{
			for(String onecode : new String[] { poscode, negcode })
			{
				DataGrabber dg = new DataGrabber(onecode, endday);
				dg.loadFromHdfs();
				dg.flushToDisk();		
			}
		} 
		
		if(lcl == LocCommLine.featcalc)
		{
			UpackScanner pos_scan = new UpackScanner.SingleFile(getSavePath(poscode, endday));
			UpackScanner neg_scan = new UpackScanner.SingleFile(getSavePath(negcode, endday));
			
			// Util.pf("Next ID is %s\n", pos_scan.peekUserId());
		
			
			PrecompFeatPack pos_pack = new PrecompFeatPack(endday, poscode);
			pos_pack.addEvalFromPack(pos_scan, 300);
			
			// BluekaiDataMan.resetSingQ();
			
			PrecompFeatPack neg_pack = new PrecompFeatPack(endday, negcode);
			neg_pack.addEvalFromPack(neg_scan, 300);
			
			FeatureReport frep = new FeatureReport(pos_pack, neg_pack);
			frep.writeToCsv("./frep_output.csv");
		}
	}
	
	public static String getSavePath(String listcode, String endday)
	{
		Util.massert(UserIndexUtil.isCanonicalDay(endday), "Must call with a canonical end day");
		return Util.sprintf("%s___%s.txt", listcode, endday);
	}
	
	private static class DataGrabber
	{
		String _endDay;
		String _listCode;
		SortedSet<String> _bigDataSet = Util.treeset();
		
		public DataGrabber(String lc, String ed)
		{
			Util.massert(UserIndexUtil.isCanonicalDay(ed), "Must call with a canonical end day");
			_listCode = lc;
			_endDay = ed;			
		}
		
		void loadFromHdfs() throws IOException
		{
			FileSystem fsys = FileSystem.get(new Configuration());
			List<String> daylist = UserIndexUtil.getCanonicalDayList(_endDay);
			
			for(String oneday : daylist)
			{
				String slicepath = UserIndexUtil.getHdfsSlicePath(oneday, _listCode);
				BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, slicepath);
				
				for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
					{ _bigDataSet.add(oneline); }

				bread.close();
				Util.pf("Finished reading file %s, datapack is now size %d\n", slicepath, _bigDataSet.size());
			}
		}
		
		void flushToDisk() throws IOException
		{
			String savepath = getSavePath(_listCode, _endDay);
			
			if(!(new File(savepath)).exists() || Util.checkOkay("Okay to delete file " + savepath))
			{
				BufferedWriter bwrite = FileUtils.getWriter(savepath);
				
				for(String oneline : _bigDataSet)
				{
					String[] toks = oneline.split("\t");
					bwrite.write(Util.joinButFirst(toks, "\t"));
					bwrite.write("\n");
				}
				
				bwrite.close();
				_bigDataSet.clear();				
			}
		}
	}
}
