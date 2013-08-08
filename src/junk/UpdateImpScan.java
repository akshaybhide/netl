
package com.adnetik.analytics;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;

// Takes the result of the Hadoop scan of the impression logs,
// and uses it to update the tracking files.
// Eventually, instead of using Tracking Files, we will want to use an RDBMS.
public class UpdateImpScan
{	
	public static File TRK_FILE = new File("/mnt/burfoot/impscan/TRK_FILE.txt");
	public static File GMP_FILE = new File("/mnt/burfoot/impscan/GMP_FILE.txt");
	
	private static final int TARG_QUEUE_SIZE = 100000;
	
	Queue<TrkImp> trkQ = new LinkedList<TrkImp>();
	Queue<RefImp> refQ = new LinkedList<RefImp>();
	
	int trkQueueSize = 0;
	int refQueueSize = 0;
	
	Scanner trkScan;
	Scanner refScan;
	PrintWriter gimp;
	
	int nextRefCalls = 0;
	int nextTrkCalls = 0;
	int trkDataOut = 0;
	double startTime = 0;
	
	public static void main(String[] args) throws Exception
	{
		if(!TRK_FILE.exists())
		{
			System.out.printf("\nMust create file TRK_FILE.txt.");	
			return;
		}
		
		UpdateImpScan uis = new UpdateImpScan();
		uis.runUpdate();
	}
	
	public UpdateImpScan() throws IOException
	{
		trkScan = new Scanner(TRK_FILE);
		refScan = new Scanner(System.in);
		
		gimp = new PrintWriter(GMP_FILE);
		
		refreshTrkQueue();
		refreshRefQueue();
		
		System.out.printf("\nInitialized TRKQ, size=%d", trkQueueSize);
		System.out.printf("\nInitialized REFQ, size=%d", refQueueSize);
	}
	
	
	public String peekTrkId()
	{
		return (trkQ.peek() == null ? null : trkQ.peek().userCampId);
	}
	
	public String peekRefId()
	{
		return (refQ.peek() == null ? null : refQ.peek().userCampId);		
	}
	
	void runUpdate() throws IOException
	{
		startTime = System.currentTimeMillis();
		
		// This loop runs until one or the other of the Qs is empty
		while(true)
		{
			String refId = peekRefId();
			String trkId = peekTrkId();
			
			if(refId == null || trkId == null)
				{ break; }
			
			int comp = trkId.compareTo(refId);
			
			TrkImp timp;			
			
			if(comp < 0) {
				
				// trkId before refId, user is not in refdata. 
				// Just re-emit the track data.
				timp = nextTrkImp();
				
			} else if (comp > 0) {
				
				// refImp is not in TrackFile
				// Create a new TrackImp based only on Refresh data
				List<RefImp> rbatch = nextRefImpBatch();
				timp = new TrkImp(rbatch);
				
			} else {
				
				// Found both the TrackImp and some Ref-Data associated with it
				// Read a TrackImp and refresh based on Ref-Data.
				timp = nextTrkImp();
				List<RefImp> rbatch = nextRefImpBatch();
				timp.updateFromSingList(rbatch);
			}
			
			printTrk(timp);			
		}
					
		// One or the other of these must be empty
		Util.massert(peekTrkId() == null || peekRefId() == null);
		
		while(peekTrkId() != null)
		{
			printTrk(nextTrkImp());
		}

		while(peekRefId() != null)
		{
			printTrk(new TrkImp(nextRefImpBatch()));
		}
		
		// Make sure we did the queue-size tracking correctly
		Util.massert(trkQueueSize == 0 && refQueueSize == 0);
		
		trkScan.close();
		refScan.close();
		gimp.close();
		
		TRK_FILE.delete();
		GMP_FILE.renameTo(TRK_FILE);
		
		double secs = (System.currentTimeMillis() - startTime)/1000;
		
		System.out.printf("\nRefCalls = %d, TrkCalls = %d", nextRefCalls, nextTrkCalls);
		System.out.printf("\nPrinted %d Track Data objects, took %.03f secs\n\n", trkDataOut, secs);		
	}
	
	void printTrk(TrkImp toprint)
	{
		trkDataOut++;	
		gimp.printf("%s\n", toprint);
		
		if((trkDataOut % 10000) == 0)
		{
			double curSecs = (System.currentTimeMillis() - startTime)/1000;
			System.out.printf("\nPrinted %d track objects, total time is %.03f", trkDataOut, curSecs);
		}					
	}
	
	TrkImp nextTrkImp()
	{
		if(trkQ.peek() == null)
			{ return null; }
		
		refreshTrkQueue();
		nextTrkCalls++;
		
		trkQueueSize--;
		return trkQ.poll();		
	}
	
	List<RefImp> nextRefImpBatch()
	{		
		//System.out.printf("\nCalling refimp... ");
		
		String nextRefId = peekRefId();
	
		if(nextRefId == null)
			{ return null; }
		
		List<RefImp> reflist = Util.vector();
		
		while(nextRefId.equals(peekRefId()))
		{
			reflist.add(refQ.poll());
			refQueueSize--;
		}
				
		refreshRefQueue();
		nextRefCalls++;
		
		//System.out.printf("... done");
		return reflist;
	}	
	
	void refreshRefQueue()
	{
		while(refQueueSize < TARG_QUEUE_SIZE && refScan.hasNextLine())
		{
			String refline = refScan.nextLine();
			
			if(refline.trim().length() > 4)
			{
				refQ.add(new RefImp(refline));
				refQueueSize++;
			}
		}				
	}
	
	void refreshTrkQueue()
	{
		while(trkQueueSize < TARG_QUEUE_SIZE && trkScan.hasNextLine())
		{
			String trkline = trkScan.nextLine();
			
			if(trkline.trim().length() > 5)
			{
				trkQ.add(new TrkImp(trkline));
				trkQueueSize++;				
			}
		}				
	}	
	
	public static class TrkImp
	{
		String userCampId;
		
		int lineIdFirst;
		int lineIdLast;
		
		String dateFirst;
		String dateLast;

		public TrkImp(String trackline)
		{
			String[] toks = trackline.split("\t");
					
			userCampId = toks[0];
			
			lineIdFirst = Integer.valueOf(toks[1]);
			dateFirst = toks[2];
			
			lineIdLast = Integer.valueOf(toks[3]);
			dateLast = toks[4];
		}
		
		public TrkImp(List<RefImp> slist)
		{
			RefImp first = slist.get(0);
			
			userCampId = first.userCampId;
			
			lineIdFirst = first.lineId;
			lineIdLast = first.lineId;
			
			dateFirst = first.timeStamp;
			dateLast = first.timeStamp;
			
			updateFromSingList(slist);
		}
		
		void updateFromSingList(List<RefImp> slist)
		{
			// TODO: need to make sure the sorting function does what we want.
			TreeMap<String, Integer> dmap = Util.treemap();
			
			dmap.put(dateFirst, lineIdFirst);
			dmap.put(dateLast, lineIdLast);
			
			for(RefImp simp : slist)
			{
				Util.massert(simp.userCampId.equals(userCampId));
				dmap.put(simp.timeStamp, simp.lineId);
			}
			
			dateFirst = dmap.firstKey();
			dateLast = dmap.lastKey();
			
			lineIdFirst = dmap.get(dateFirst);
			lineIdLast = dmap.get(dateLast);
		}
		
		public String toString()
		{
			return Util.sprintf("%s\t%d\t%s\t%d\t%s",
				userCampId, lineIdFirst, dateFirst, lineIdLast, dateLast);
		}
	}
	
	public static class RefImp
	{
		String userCampId;
		int lineId;
		String timeStamp; // TODO, make this a Date
		
		
		public RefImp(String logline)
		{
			String[] toks = logline.split("\t");
			userCampId = toks[0];
			lineId = Integer.valueOf(toks[1]);
			timeStamp = toks[2];
		}
	}
}
