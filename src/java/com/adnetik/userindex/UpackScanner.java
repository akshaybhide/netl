
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.UserIndexUtil.*;

// TODO: all this scanner code should be refactored
// We want ability to read from multiple files and just a single file,
// but we don't need to replicate this with SlicexxPackScanner
public abstract class UpackScanner 
{
	protected static String ID_FIELD_NAME = "wtp_user_id";
	
	protected LogType reltype;
	protected LogVersion relvers;
	
	// Use this or not?
	private boolean isFinished = false;
	
	public abstract UserPack next() throws IOException;
	public abstract String peekUserId();

	// public abstract int getExCount();
	// public abstract int getNoIdCount(); 
	
	public boolean hasNext()
	{
		return (peekUserId() != null);
	}
				
	public static class SingleFile extends UpackScanner
	{
		SortedFileMap _sortFile;
		
		// pixel_8580_____00198ada-6c09-44b5-93bc-71c862b74654
		
		public SingleFile(File targfile) throws IOException
		{
			_sortFile = SortedFileMap.buildFromFile(targfile, false);
		}
		
		public String peekUserId()
		{
			String pkey = _sortFile.firstKey();
			
			return pkey.split(Util.DUMB_SEP)[1];
		}
		
		
		public UserPack next() throws IOException
		{
			Map.Entry<String, List<String>> myentry = _sortFile.pollFirstEntry();
			
			String[] listcode_wtp = myentry.getKey().split(Util.DUMB_SEP);
			WtpId userwtp = (listcode_wtp.length < 2 ? null : WtpId.getOrNull(listcode_wtp[1]));
			
			UserPack upack = new UserPack();
			upack.userId = listcode_wtp[1];
			
			Iterator<String> peelit = getPeelIterator(myentry.getValue().iterator());
			
			ScoreUserJob.popUPackFrmRdcrData(upack, peelit);
			
			return upack;
		}
		
		private Iterator<String> getPeelIterator(final Iterator<String> srcit)
		{			
			return new Iterator<String>() { 
				
				public boolean hasNext() {
					return srcit.hasNext();	
				}
				
				
				public String next() {
					String srcnext = srcit.next();
					return Util.splitOnFirst(srcnext, "\t")._2;
				}
				
				public void remove() { Util.massert(false); }
				
				// public void remove() { Util.massert
				
			};
		}
		
		public boolean hasNext()
		{
			return _sortFile.hasNext();
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		/*
		UpackScanner uscan = new SingleFile(new File("specUS.sort"));
		
		List<BinaryFeature<UserPack>> iablist = StrayerFeat.getIabFeatList();
		Util.pf("Found %d IAB features\n", iablist.size());
				
		while(uscan.hasNext())
		{
			UserPack upack = uscan.next();	
			
			Util.pf("Found %d bids, %d pixels for user %s\n", 
				upack.getCalloutCount(), upack.getPixList().size(), upack.userId);
			
			for(BinaryFeature<UserPack> onefeat : iablist)
			{
				if(onefeat.eval(upack) == EvalResp.T)
				{
					Util.pf("\tFeature %s fired for user\n", onefeat);
				}
			}
		}
		*/
	}
}
