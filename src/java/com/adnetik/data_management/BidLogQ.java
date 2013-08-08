
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.data_management.SegmentPathMan.*;


// This a combined code for several sub-Qs: IAB, IP, and domain
public class BidLogQ
{
	public static class IppQueue extends SegmentDataQueue
	{		
		private static IppQueue _SING_QUEUE;
		
		public static void setSingQ(String daycode) throws IOException
		{
			Util.massert(false, "NYI");
			// setSingQ(getHdfsSingQ(daycode));
		}
				
		public static synchronized boolean isQReady()
		{
			return (_SING_QUEUE != null);
		}
		
		public static void setSingQ(IppQueue myqueue) throws IOException
		{
			Util.massert(_SING_QUEUE == null, "IP Queue already initialized");
			_SING_QUEUE = myqueue;
		}
		
		public static synchronized void unsetSingQ()
		{
			_SING_QUEUE = null;
		}
		
		public static synchronized IppQueue getSingQ() throws IOException
		{
			Util.massert(_SING_QUEUE != null, "Must set the Queue first!!");
			return _SING_QUEUE;
		}
		
		public Party3Type getDataSetCode()
		{
			return Party3Type.ipp_ourdata;	
		}
		
		public static synchronized void resetSingQ(IppQueue myqueue) throws IOException
		{
			_SING_QUEUE = myqueue;	
		}		
		
		private boolean _isNullMode = false;
		
		public IppQueue(LineReader bread) throws IOException
		{
			super(Party3Type.ipp_ourdata, bread);
		}		
		
		public IppQueue() throws IOException
		{
			super(Party3Type.ipp_ourdata);			
		}

		public void setNullMode()
		{
			_isNullMode = true;	
		}
		
		public IPUserPack lookup(String userid) throws IOException
		{
			if(_isNullMode)
				{ return null; }			
			
			Map.Entry<String, List<String>> myentry = lookupEntry(userid);
			return (myentry == null ? null : IPUserPack.build(myentry));
		}

		public IPUserPack nextPack() throws IOException
		{
			Util.massert(!_isNullMode, "Attempt to call nextPack(..) in null mode");
			return IPUserPack.build(nextEntry());
		}		
		
		@Override
		public IPUserPack buildEmpty(String wtpid)
		{
			return new IPUserPack(wtpid);	
		}
	}
	
	public static class IPUserPack extends SegmentPack<Long>
	{
		// TODO: is there a point to keeping these error lists around?
		List<String> errlist = Util.vector();
		
		IPUserPack(String wtpid)
		{
			super(wtpid);	
		}
		
		IPUserPack(Map.Entry<String, List<String>> myentry)
		{
			super(myentry);
		}
			
		private static IPUserPack build(Map.Entry<String, List<String>> myentry)
		{
			return new IPUserPack(myentry);
		}
		
		public Long getSegId(String oneseg)
		{
			return Util.ip2long(oneseg);	
		}
		
		@Override
		public String segId2String(Long segid)
		{
			return Util.long2ip(segid);	
		}		
	}		
	
	
	public static class IabQueue extends SegmentDataQueue
	{
		private boolean _isNullMode = false;
				
		private static IabQueue _SING_QUEUE;
		
		public static void setSingQ(String daycode) throws IOException
		{
			Util.massert(false, "Not yet implemented");
			// setSingQ(getHdfsSingQ(daycode));
		}
		
		public static void setLocalSingQ(String daycode) throws IOException
		{
			throw new RuntimeException("NYI");
		}
		
		public static synchronized boolean isQReady()
		{
			return (_SING_QUEUE != null);
		}
		
		public static void setSingQ(IabQueue myqueue) throws IOException
		{
			Util.massert(_SING_QUEUE == null, "IP Queue already initialized");
			_SING_QUEUE = myqueue;
		}
		
		public static synchronized void unsetSingQ()
		{
			_SING_QUEUE = null;
		}
		
		public static synchronized IabQueue getSingQ() throws IOException
		{
			Util.massert(_SING_QUEUE != null, "Must set the Queue first!!");
			return _SING_QUEUE;
		}
				
		public static synchronized void resetSingQ(IabQueue myqueue) throws IOException
		{
			_SING_QUEUE = myqueue;	
		}				
		
		public IabQueue(LineReader bread) throws IOException
		{
			super(Party3Type.iab_ourdata, bread);
		}	
		
		public IabQueue() throws IOException
		{
			super(Party3Type.iab_ourdata);
		}		
		

		public void setNullMode()
		{
			_isNullMode = true;	
		}
		
		public IabUserPack lookup(String userid) throws IOException
		{
			if(_isNullMode)
				{ return null; }			
			
			Map.Entry<String, List<String>> myentry = lookupEntry(userid);
			return (myentry == null ? null : IabUserPack.build(myentry));
		}

		public IabUserPack nextPack() throws IOException
		{
			Util.massert(!_isNullMode, "Attempt to call nextPack(..) in null mode");
			return IabUserPack.build(nextEntry());
		}	

		@Override
		public IabUserPack buildEmpty(String wtpid)
		{
			return new IabUserPack(wtpid);	
		}
	}
	
	public static class IabUserPack extends SegmentPack.IntPack
	{
		List<String> errlist = Util.vector();
		
		IabUserPack(String wtpid)
		{
			super(wtpid);	
		}
		
		IabUserPack(Map.Entry<String, List<String>> myentry)
		{
			super(myentry);
		}
			
		private static IabUserPack build(Map.Entry<String, List<String>> myentry)
		{
			return new IabUserPack(myentry);
		}
	}		
	
	public static class PixQueue extends SegmentDataQueue
	{
		private boolean _isNullMode = false;
						
		public PixQueue(LineReader bread) throws IOException
		{
			super(Party3Type.pix_ourdata, bread);
		}	
		
		public PixQueue() throws IOException
		{
			super(Party3Type.pix_ourdata);
		}		
		

		public void setNullMode()
		{
			_isNullMode = true;	
		}
		
		public PixelPack lookup(String userid) throws IOException
		{
			if(_isNullMode)
				{ return null; }			
			
			Map.Entry<String, List<String>> myentry = lookupEntry(userid);
			return (myentry == null ? null : PixelPack.build(myentry));
		}

		public PixelPack nextPack() throws IOException
		{
			Util.massert(!_isNullMode, "Attempt to call nextPack(..) in null mode");
			return PixelPack.build(nextEntry());
		}	

		@Override
		public PixelPack buildEmpty(String wtpid)
		{
			return new PixelPack(wtpid);	
		}
	}
	
	public static class PixelPack extends SegmentPack.IntPack
	{
		PixelPack(String wtpid)
		{
			super(wtpid);	
		}
		
		PixelPack(Map.Entry<String, List<String>> myentry)
		{
			super(myentry);
		}
			
		private static PixelPack build(Map.Entry<String, List<String>> myentry)
		{
			return new PixelPack(myentry);
		}
	}			
}
