
package com.adnetik.userindex;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;

public class InMemoryScan extends Configured implements Tool
{	
	private static final int TARG_SIZE = 10000;
	
	public static final int NUM_INTEREST_DAYS = 3;
	
	// TODO: is treemap or hashmap better?
	Map<String, List<Short>> pixelMap = Util.treemap();
	
	String pixelPrefPath;
	Map<Integer, Writer> pixelDataWriter = Util.hashmap();	
	
	FileSystem fSystem;
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new InMemoryScan(), args);
		System.exit(exitCode);
	}
	
	public BufferedReader gimmeReader() throws IOException
	{
		String filename = "ad_test.log.gz";
		
        	InputStream fileStream = new FileInputStream(filename);
        	InputStream gzipStream = new java.util.zip.GZIPInputStream(fileStream);		
		
       		BufferedReader bread = new BufferedReader(new InputStreamReader(gzipStream, BidLogEntry.BID_LOG_CHARSET)); 	
        	
		//BufferedReader bread = new BufferedReader(new InputStreamReader(new FileInputStream("ad_test.log"), 
		//	 BidLogEntry.BID_LOG_CHARSET));
		
		return bread;
	}
	

	
	void buildPixelMap() throws IOException
	{
		Util.pf("\nBuilding pixel map file ... ");		
		List<String> daylist = TimeUtil.getDateRange(NUM_INTEREST_DAYS);
		
		for(String daycode : daylist)
		{
			String pospath = Util.sprintf("/data/analytics/userindex/interest/day_%s.txt", daycode);
			String negpath = Util.sprintf("/data/analytics/userindex/interest/neg_%s.txt", daycode);
			
			Util.pf("\nSlurping path %s", pospath);
			insertIntoPixelMap(HadoopUtil.hdfsScanner(fSystem, pospath));
			Util.pf("\nSlurping path %s", negpath);
			insertIntoPixelMap(HadoopUtil.hdfsScanner(fSystem, negpath));
			Util.pf("\n... done");
		}
		Util.pf("\nDone building pixel map");	
	}
	
	void insertIntoPixelMap(Scanner intScan)
	{
		while(intScan.hasNextLine())
		{
			String logline = intScan.nextLine().trim();
			InterestPack ipack = new InterestPack(logline);
			
			if(!pixelMap.containsKey(ipack.wtp))
				{ pixelMap.put(ipack.wtp, new LinkedList<Short>()); }
			
			pixelMap.get(ipack.wtp).add(ipack.pix);
		}		
		
		Util.showMemoryInfo();
	}
	
	public int run(String[] args) throws IOException
	{
		String daycode = args[0];
		
		if("yest".equals(daycode))
			{ daycode = TimeUtil.getYesterdayCode(); }
		
		pixelPrefPath = args[1];
		
		fSystem = FileSystem.get(getConf());
		cleanPixelDir();
		
		buildPixelMap();
		
		processBatch(ExcName.admeld, LogType.no_bid_all, "2011-11-26");
				
		return 1;
	}

	void processBatch(ExcName excname, LogType logtype, String daycode) throws IOException
	{
		String glob = Util.sprintf("/data/%s/%s/%s/*.log.gz", logtype, daycode, excname);		
		List<Path> batchlist = HadoopUtil.getGlobPathList(fSystem, glob);
		
		Util.pf("\nFound %d batch files for glob %s", batchlist.size(), glob);
		
		for(Path onepath : batchlist)
		{
			BufferedReader bread = gimmeHdfsReader(onepath);
			processData(bread);
		}
	}
	
	void processData(BufferedReader bread) throws IOException
	{
		// Okay, now time the file read
		double startTime = System.currentTimeMillis();
		int logLinesRead = 0;
		int exCount = 0;
		
		int wtpIdField = BidLogEntry.getFieldId(LogType.no_bid_all, LogVersion.v13, "wtp_user_id");
		
		for(String line = bread.readLine(); ; line = bread.readLine())
		{
			if(line == null || line.trim().length() == 0)
				{ break; }
			
			logLinesRead++;
			//Util.pf("\nRead line %d", logLinesRead);
			
			if((logLinesRead % 1000) == 0)
			{
				//Util.pf("\nRead %d loglines, %d exceptions", logLinesRead, exCount);
			}
			
			String[] toks = line.split("\t");
			
			if(toks.length <= wtpIdField)
			{
				exCount++;
				continue;
			}
			
			String wtp = toks[wtpIdField];
			
			if(pixelMap.containsKey(wtp))
			{
				for(int pixid : pixelMap.get(wtp))
				{
					emit2pixFile(pixid, line);
				}
			}
		}
		
		// Okay, now time the file read
		Util.pf("\nTook %.02f seconds to process %d lines", (System.currentTimeMillis() - startTime)/1000, logLinesRead);
		bread.close();
	}		
	
	public BufferedReader gimmeHdfsReader(Path hdfsPath) throws IOException
	{
        	InputStream gzipStream = new java.util.zip.GZIPInputStream(fSystem.open(hdfsPath));		
       		BufferedReader bread = new BufferedReader(new InputStreamReader(gzipStream, BidLogEntry.BID_LOG_CHARSET)); 	
		return bread;
	}		
	
	void emit2pixFile(int pixid, BidLogEntry ble) throws IOException
	{
		emit2pixFile(pixid, ble.getLogLine());
	}	
	
	void emit2pixFile(int pixid, String logline) throws IOException
	{
		if(!pixelDataWriter.containsKey(pixid))
		{
			String pathname = getPixelFile(pixid);
			Writer pw;
			try { pw = new BufferedWriter(new FileWriter(pathname)); }
			catch (FileNotFoundException fnex) {
				throw new RuntimeException(fnex);	
			}
			
			pixelDataWriter.put(pixid, pw);
		}
		
		pixelDataWriter.get(pixid).write(logline);
		pixelDataWriter.get(pixid).write("\n");
	}		
	
	
	String getPixelFile(int pixid)
	{
		return Util.sprintf("%s/pixel_data%d.txt", pixelPrefPath, pixid);
	}	
	
	void cleanPixelDir()
	{
		int delfiles = 0;
		File pixdir = new File(pixelPrefPath);
		
		if(pixdir.listFiles() == null)
			{ return; }
		
		for(File f : pixdir.listFiles())
		{
			if(f.getName().indexOf("pixel_data") > -1)
			{
				f.delete();
				//Util.pf("\nFile is %s", f);	
				delfiles++;
			}
		}
		
		Util.pf("\nDeleted %d pixel_data files", delfiles);
	}
	
	public static class InterestPack
	{
		String wtp;
		Short pix;
		
		public InterestPack(String line)
		{
			String[] reskey_cnt = line.trim().split("\t");
			String[] wtp_pix = reskey_cnt[0].split(Util.DUMB_SEP);

			wtp = wtp_pix[0];
			int longpix = Integer.valueOf(wtp_pix[1]);
			
			if(longpix < Short.MAX_VALUE)
				{ pix = (short) longpix; }
			
			else {
				// BIGTODO: cut this back
				longpix = (longpix % 1000);
				pix = (short) -longpix;
			}
		}
	}
}
