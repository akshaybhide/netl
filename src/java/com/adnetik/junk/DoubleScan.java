
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
import com.adnetik.shared.BidLogEntry.LogType;

import com.adnetik.analytics.*;

public class DoubleScan extends Configured implements Tool
{	
	private static final int TARG_SIZE = 10000;
	
	LinkedList<BidLogEntry> logQ = Util.linkedlist();
	LinkedList<InterestPack> intQ = Util.linkedlist();
	
	int intQsize = 0;
	int logQsize = 0;
	
	Scanner logScan;
	Scanner intScan;
	
	int logLinesRead = 0;
	int intLinesRead = 0;
	
	String pixelPrefPath;
	Map<Integer, Writer> pixelDataWriter = Util.treemap();
	
	// NUmber of exceptions observed in log file
	int excount = 0;
	int emitCount = 0; // number of "hits"
	
	LogType relType = LogType.no_bid_all;
	
	public static void main(String[] args) throws IOException
	{
		DoubleScan ds = new DoubleScan();
		ds.run(args);
	}
	
	public int run(String[] args) throws IOException
	{
		String interestFilePath = args[0];
		String logFilePath = args[1];
		pixelPrefPath = args[2];
		
		intScan = new Scanner(new File(interestFilePath));
		logScan = new Scanner(new File(logFilePath), BidLogEntry.BID_LOG_CHARSET);		
		
		double startTime = System.currentTimeMillis();
		Util.massert(logScan.hasNextLine(), "Cannot read from log scanner");
		
		refIntQ();
		refLogQ();
		
		Util.massert(!logQ.isEmpty(), "Error: log file is empty %s", logFilePath);
		Util.massert(!intQ.isEmpty(), "Error: interest file is empty %s", interestFilePath);
		
		Util.pf("\nInt size is %d, log size is %d", intQ.size(), logQ.size());
		
		BidLogEntry ble = nextBle();
		TreeMap<String, List<Integer>> pixpack = nextPixPack();
		
		while(true)
		{
			if(logQ.isEmpty() || intQ.isEmpty())
			{
				// done - there aren't going to be any more matches
				break; 	
			}

			int compare = ble.getField("wtp_user_id").compareTo(pixpack.firstKey());
			
			if(compare < 0)
			{
				ble = nextBle();
			}
			else if(compare > 0)
			{
				pixpack = nextPixPack();	
			} 
			else // compare == 0 implies IDs are the same
			{
				// output the ble to all the pixel files
				for(Integer pixid : pixpack.get(pixpack.firstKey()))
				{
					emit2pixFile(pixid, ble);
					emitCount++;
					//Util.pf("\nOutputting to pixel %d", pixid);					
				}
				
				if(!logQ.isEmpty())
					{ ble = nextBle(); }
			}
		}

		logScan.close();
		intScan.close();
		
		for(int pixid : pixelDataWriter.keySet())
		{
			pixelDataWriter.get(pixid).close();
		}
		
		Util.pf("\nDoubleScan finished, statistics:\n\tRead %d log and %d interest lines",
			logLinesRead, intLinesRead, excount);
		Util.pf("\n\tFound %d log format exceptions", excount);
		Util.pf("\n\tEmitted %d data lines to pixel files", emitCount);
		Util.pf("\n\tTook %.02f seconds", (System.currentTimeMillis()-startTime)/1000);
		
		Util.pf("\n------\n");

		return 0;
	}
	
	void emit2pixFile(int pixid, BidLogEntry ble) throws IOException
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
		
		pixelDataWriter.get(pixid).write(ble.getLogLine());
		pixelDataWriter.get(pixid).write("\n");
	}
	
	String getPixelFile(int pixid)
	{
		return Util.sprintf("%s/pixel_data%d.txt", pixelPrefPath, pixid);
	}
	
	public BidLogEntry nextBle()
	{
		Util.massert(!logQ.isEmpty(), "Log Queue is empty when nextBle called");
		
		BidLogEntry ble = logQ.poll();
		logQsize--;
		refLogQ();
		
		Util.massertEq(logQsize, logQ.size());
		
		return ble;
	}
	
	public TreeMap<String, List<Integer>> nextPixPack()
	{
		TreeMap<String, List<Integer>> pixmap = Util.treemap();
		List<Integer> pixlist = Util.vector();
		
		InterestPack ip = intQ.poll();
		pixlist.add(ip.pix);
		pixmap.put(ip.wtp, pixlist);
		
		while(!intQ.isEmpty() && ip.wtp.equals(nextIntId()))
		{
			InterestPack nextip = intQ.poll();
			pixmap.get(nextip.wtp).add(nextip.pix);
		}
		
		intQsize -= pixlist.size();
		
		// Refresh the Interest Queue
		refIntQ();
		
		Util.massertEq(intQsize, intQ.size());
		return pixmap;
	}
	
	public String nextLogId()
	{
		return logQ.peek().getField("wtp_user_id");
	}
	
	
	public String nextIntId()
	{
		return intQ.peek().wtp;
	}
	
	private void refIntQ()
	{
		while(intScan.hasNextLine() && intQsize < TARG_SIZE)
		{
			String line = intScan.nextLine().trim();
			intLinesRead++;			
			
			InterestPack ip = new InterestPack(line);
			
			intQ.add(ip);
			intQsize++;
		}
	}
	
	private void refLogQ()
	{
		while(logScan.hasNextLine() && logQsize < TARG_SIZE)
		{
			String line = logScan.nextLine();
			logLinesRead++;
			//Util.pf("\nRead line %d", logLinesRead);
			
			BidLogEntry ble;
			
			try {
				ble = new BidLogEntry(LogType.no_bid_all, line);	
				ble.strictCheck();
				//ble.wtpIdCheck(); // TODO: maybe too slow
				
			} catch (BidLogEntry.BidLogFormatException blex) {
				
				excount++;
				continue;
			}
			
			String wtp = ble.getField("wtp_user_id");
		
			if(wtp.trim().length() == 0)
				{ continue; }
			
			logQ.add(ble);
			logQsize++;		
		}
		
		//Util.pf("\nFinished logQ refresh, found %d exceptions, size is %d", excount, logQsize);
		
		if(!logScan.hasNextLine())
		{
			//Util.pf("\nFinished scan of log file");
		}
	}
	
	
	public static class InterestPack
	{
		String wtp;
		Integer pix;
		
		public InterestPack(String line)
		{
			String[] reskey_cnt = line.trim().split("\t");
			String[] wtp_pix = reskey_cnt[0].split(Util.DUMB_SEP);

			wtp = wtp_pix[0];
			pix = Integer.valueOf(wtp_pix[1]);
		}
	}
}
