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
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;

import com.adnetik.analytics.*;	

// This is a modified version UserPack.UpackScanner. 
// TODO: is this necessary? How is this intended to be used?
public class SlicePackScanner extends UpackScanner
{		
	// ID we're looking for.
	String listCode;
			
	SortedSet<String> nextUsers = Util.treeset();	
	
	private static int TARG_SIZE = 20000;
	
	private SortedMap<String, SubPackScanner> partFileScanMap = Util.treemap();		
	
	public SlicePackScanner(LogType rtype, LogVersion rvers, String lcode)
	{
		reltype = rtype;
		relvers = rvers;
		
		listCode = lcode;
		
		Util.pf("\ninitializing slice pack scanner");
	}
	
	public static void main(String[] args) throws Exception
	{
		// downloadNSort("pixel_10090");
		SlicePackScanner sps = SlicePackScanner.buildCanonical("pixel_10090");
		
		while(sps.hasNext())
		{
			UserPack up = sps.next();
			
			for(String funcname : StrayerFeat.getFeatMap().keySet())
			{
				BinaryFeature<UserPack> bfeat = StrayerFeat.getFeatMap().get(funcname);
				
				if(bfeat.eval(up).getVal())
				{
					if(bfeat.getCode().equals("demographic"))
						{ Util.pf("Feature %s is true for up=%s\n", funcname, up.userId); }
				}
			}
		}
	}
	
	
	// call downloadNSort first
	public static SlicePackScanner buildCanonical(String listcode)
	{
		SlicePackScanner sps = new SlicePackScanner(LogType.UIndexMinType, LogVersion.UIndexMinVers, listcode);
		
		try { 
			for(String oneday : UserIndexUtil.getCanonicalDayList())
				{ sps.addLocalSliceReader(oneday); }
		} catch (IOException ioex) {
			throw new RuntimeException(ioex);	
		}
		
		sps.refreshUserStack();
		
		return sps;
	}
	
	public static void downloadNSort(String listcode) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		
		for(String oneday : UserIndexUtil.getCanonicalDayList())
		{
			Path hdfspath = new Path(UserIndexUtil.getHdfsSlicePath(oneday, listcode));
			Path loclpath = new Path(localSliceCopy(listcode, oneday));
			
			fsys.copyToLocalFile(hdfspath, loclpath);
			Util.unixsort(loclpath.toString(), "dummy_sort", "");
		}
	}
	
	public static String localSliceCopy(String listcode, String daycode)
	{
		return Util.sprintf("local_cp_%s_%s.slice", listcode, daycode);
	}
	
	public String peekUserId()
	{
		return (nextUsers.isEmpty() ? null : nextUsers.first());
	}
	
	public boolean hasNext()
	{
		return !nextUsers.isEmpty();	
	}
	
	public UserPack next()
	{		
		String nextUserId = nextUsers.first();
		
		UserPack nextUser = new UserPack();
		nextUser.userId = nextUserId;
		
		for(String daycode : partFileScanMap.keySet())
		{
			int prev = nextUser.getData().size();
			// Add all the callouts from the given daycode
			partFileScanMap.get(daycode).addToUpack(nextUser);		
			int post = nextUser.getData().size();
			
			/*
			if((post-prev) > 1000)
			{
				Util.pf("\nFound %d callouts for user %s from daycode %s",
					(post - prev), nextUserId, daycode);
			}
			*/
		}
		
		// Pull it off the stack
		nextUsers.remove(nextUserId);
		
		refreshUserStack(nextUserId);
		
		return nextUser;
	}		
	
	int countUsers()
	{
		// This should just go through calling next()
		throw new RuntimeException("not yet implemented");	
	}
	
	@Override
	public int getNoIdCount()
	{
		int t = 0;
		for(String code : partFileScanMap.keySet())
		{
			t += partFileScanMap.get(code).subNoIdCount;	
		}
		return t;
	}
	
	@Override
	public int getExCount()
	{
		int t = 0;
		for(String code : partFileScanMap.keySet())
		{
			t += partFileScanMap.get(code).subExCount;	
		}
		return t;		
	}	
	
	void addReader(String daycode, BufferedReader bread)
	{
		Util.massert(!partFileScanMap.containsKey(daycode));
		partFileScanMap.put(daycode, new SubPackScanner(bread));
	}
	
	void addLocalSliceReader(String daycode) throws IOException
	{
		String slicepath = localSliceCopy(listCode, daycode);
		addReader(daycode, Util.getReader(slicepath));
	}
	
	void refreshUserStack()
	{
		refreshUserStack(Util.WTP_ZERO_ID);
	}
	
	void refreshUserStack(String prevId)
	{
		for(String daycode : partFileScanMap.keySet())
		{
			String newid = partFileScanMap.get(daycode).peekUserId();
			
			// Can be null if the stack is empty
			if(newid == null)
				{ continue; }
			
			// New id must be AFTER previous one
			Util.massert(prevId.compareTo(newid) < 0, "Prev is %s, new is %s", prevId, newid);
			
			nextUsers.add(newid);
		}		
	}		
	
	private class SubPackScanner
	{
		boolean printHitSweet = false;
		boolean readFinished = false;
		
		
		// These should be zero, right...??
		int subExCount = 0;
		int subNoIdCount = 0;
		
		int qSize = 0;
		LinkedList<BidLogEntry> qLines = Util.linkedlist();
		
		BufferedReader breader;	
		
		public SubPackScanner(BufferedReader br)
		{
			breader = br;	
			refQ();
		}
		
		public String peekUserId()
		{
			// This ID_FIELD_NAME is pretty hokey
			return (qLines.isEmpty() ? null : qLines.peek().getField(ID_FIELD_NAME));
		}
		
		public void addToUpack(UserPack upack)
		{
			Util.massert(upack.userId != null, "Must set userId before calling addToUpack");
			
			while(upack.userId.equals(peekUserId()))
			{
				upack.add(qLines.poll(), ID_FIELD_NAME);
				qSize--;
			}
			
			// This means we exhausted the entire buffer for a single user, 
			// so we'll have to refresh and start again. 
			boolean doItAgain = (qSize == 0 && !readFinished);
			
			refQ();
			
			if(doItAgain)
			{
				Util.pf("\nRequired multiple add steps for user %s", upack.userId);
				addToUpack(upack); 
			}
		}
		
		private void refQ() 
		{
			//System.out.printf("\nCalling refQ, qsize=%d", qsize);
			
			try {
				while(qSize < TARG_SIZE)
				{
					String full_line = breader.readLine();
					
					if(full_line == null)
					{ 
						readFinished = true;
						break; 
					}
					
					int ftab = full_line.indexOf("\t");
					
					// Lines are prefixed with a padded pixel id, and the wtp id.
					// example: 00001282_____addf17c7-7945-4a95-9303-f723828c503f
					String idinfo = full_line.substring(0, ftab);
					String[] list_wtp = idinfo.split(Util.DUMB_SEP);
					String pixid = list_wtp[0];
					
					Util.massert(listCode.equals(list_wtp[0]), "Unexpected listcode found in file: %s", listCode);
					
					// This is the ORIGINAL line in the bid log file
					String logline = full_line.substring(ftab+1);
					
					try {
						BidLogEntry ble = new BidLogEntry(reltype, relvers, logline);
						
						if(ble.getField(UpackScanner.ID_FIELD_NAME).trim().length() > 0)
						{	
							qLines.add(ble);
							qSize++;
						} else {
							subNoIdCount++;	
						}
						
						
					} catch (BidLogEntry.BidLogFormatException ex) {
						subExCount++;	
						//System.out.printf("\nline is %s", oneline);
						// ex.printStackTrace();
					}
				}	
			} catch (IOException ioex) {
				
				throw new RuntimeException(ioex);	
			}
			
			//System.out.printf("\nFinished refQ, qsize=%d", qsize);
		}		
	}	
}


