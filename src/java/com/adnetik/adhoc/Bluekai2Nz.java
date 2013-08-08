
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.BmUtil.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


// This is a test uploader to send Bluekai data to Netezza
public class Bluekai2Nz
{
	private static String BK_GIMP_FILE_PATH = "/home/burfoot/netezza/bkupload/gimp.txt";
	
	private BufferedWriter _curWriter;
	
	private int _curBatchCount = 0;
	private int _totalRecCount = 0;
	
	public static void main(String[] args) throws Exception
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		BufferedReader bread = 	HadoopUtil.hdfsBufReader(fsys, "/thirdparty/bluekai/snapshot/MASTER_LIST_2013-03-12.txt");
		
		Bluekai2Nz bk2nz = new Bluekai2Nz();
		
		int lcount = 0;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			bk2nz.openIfNecessary();
			
			String[] wtp_date_seglist = oneline.split("\t");
			WtpId wid = WtpId.getOrNull(wtp_date_seglist[0]);
			if(wid == null)
				{ continue; }
			
			String daycode = wtp_date_seglist[1];
			
			// Util.pf("Oneline is %s\n", oneline);
			
			lcount++;
			
			for(String oneseg : wtp_date_seglist[2].split(","))
			{
				bk2nz.sendRecord(wid, daycode, Integer.valueOf(oneseg.trim()));
			}
			
			
			if(bk2nz._curBatchCount > 1000000)
			{
				Util.pf("sending data, curid is %s, total rec is %d, curbatch is %d\n", 
					wid, bk2nz._totalRecCount, bk2nz._curBatchCount);
				
				bk2nz.flushData();
			}
		}
		
		bread.close();
		
		bk2nz.flushData();
		
	}
	
	private void sendRecord(WtpId wid, String daycode, int segid) throws IOException
	{
		
		FileUtils.writeRow(_curWriter, "\t", wid, daycode, segid);
		_curBatchCount++;
		_totalRecCount++;
	}
	
	private void openIfNecessary() throws IOException
	{
		if(_curWriter == null)
		{
			_curWriter = FileUtils.getWriter("/home/burfoot/netezza/bkupload/gimp.txt");
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
		
		TestNzUpload.transferFile(new File(BK_GIMP_FILE_PATH), new SimpleMail("gimp"));
		TestNzUpload.remoteUploadCommand(BK_GIMP_FILE_PATH, "bk_test_1", new SimpleMail("gimp"));
		
		_curBatchCount = 0;
		
		// System.exit(1);
	}
}
