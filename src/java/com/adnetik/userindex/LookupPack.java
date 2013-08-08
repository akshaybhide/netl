
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;


import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;
// import com.adnetik.analytics.InterestUserUpdate;
import com.adnetik.userindex.UserIndexUtil.StagingType;


public class LookupPack
{	
	private Map<WtpId, Set<String>> _lookupMap = Util.treemap();
	private Map<String, Integer> _listCountMap = Util.treemap();
	
	private static LookupPack _SING;
	
	private static Set<String> _targCodes;
	
	private Double _lookupTimeSecs = 0D;
	
	private String _blockStart; 
	
	public LookupPack(String dc)
	{
		UserIndexUtil.assertValidBlockStart(dc);
		_blockStart = dc;
	}
	
	public long getLookupTimeSecs()
	{
		return Math.round(_lookupTimeSecs);
	}
	
	public synchronized void readFromHdfs(FileSystem fsys) throws IOException
	{	
		Util.massert(stagingInfoReady(fsys),
			"Staging info for blockstart=%s is not yet ready; must wait", _blockStart);
		
		TreeMap<String, Integer> manimap = loadManifest(fsys);
		
		List<ScanRequest> sclist = grabStagingListCodes(fsys, _blockStart);
		
		for(ScanRequest screq : sclist)
		{
			Util.massert(manimap.containsKey(screq.getListCode()), 
				"No manifest entry found for %s", screq.getListCode());
		}
		
				
		double startup = Util.curtime();
		
		for(ScanRequest scanreq : sclist)
		{
			loadScanRequestData(fsys, scanreq);
			Util.pf(".");
			
			{
				Util.massert(manimap.containsKey(scanreq.getListCode()),
					"No manifest entry found for listcode %s", scanreq.getListCode());
				
				int mcount = manimap.remove(scanreq.getListCode());
				int lcount = _listCountMap.get(scanreq.getListCode());
				Util.massert(mcount == lcount || lcount == UserIndexUtil.MAX_USERS_PER_LIST,
					"Found %d cookies in manifest file but %d in list for listcode %s",
					mcount, lcount, scanreq.getListCode());
			}
		}
		
		// This should now be empty
		Util.massert(manimap.isEmpty(), 
			"Manifest has cookie lists not loaded by system: %s", manimap.keySet());
		
		_lookupTimeSecs = (Util.curtime() - startup)/1000;
	}	
	
	// This code can be tested independently to make sure all the cookie lists are
	// actually in the ListInfoManager
	static List<ScanRequest> grabStagingListCodes(FileSystem fsys, String daycode) throws IOException
	{
		List<ScanRequest> cklist = Util.vector();
		String stagepatt = Util.sprintf("/userindex/staging/%s/*.cklist", daycode);		
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, stagepatt);
		Util.pf("Found %d paths matching pattern %s\n", pathlist.size(), stagepatt);
		
		for(Path onepath : pathlist)
		{
			String fname = onepath.getName();
			String listcode = fname.split("\\.")[0];
			if(!ListInfoManager.getSing().haveRequest(listcode))
			{
				String errmssg = Util.sprintf("Listcode %s not found for cookie list %s",
									listcode, onepath);
				
				throw new IllegalArgumentException(errmssg);	
			}
			
			cklist.add(ListInfoManager.getSing().getRequest(listcode));
		}		
		
		return cklist;
	}
	
	
	void write2File(File targfile) throws IOException
	{
		BufferedWriter bwrite = FileUtils.getWriter(targfile.getAbsolutePath());
		
		for(Map.Entry<WtpId, Set<String>> oneidpair : _lookupMap.entrySet())
		{
			WtpId wid = oneidpair.getKey();
			
			for(String onelc : oneidpair.getValue())
				{ FileUtils.writeRow(bwrite, "\t", wid, onelc); }
		}
		
		bwrite.close();
	}
	
	int loadScanRequestData(FileSystem fsys, ScanRequest scanreq) throws IOException
	{
		// Number of exceptions
		int excount = 0;
		int lcount = 0;
			
		String hdfspath = UserIndexUtil.getStagingInfoPath(scanreq, _blockStart);
		
		BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, hdfspath);
		
		for(String oneline = bread.readLine(); oneline != null && lcount < UserIndexUtil.MAX_USERS_PER_LIST; oneline = bread.readLine())
		{
			String wtpstr = oneline.trim();
			
			WtpId shortform = null;
			
			try {  shortform = new WtpId(wtpstr); }
			catch (Exception ex) { excount++; } 
			
			if(shortform == null)
				{ continue; }
			
			String listcode = scanreq.getListCode();
			
			// If this is set, we want to SKIP some listcodes 
			if(_targCodes != null && !_targCodes.contains(listcode))
				{ continue; }
						
			Util.setdefault(_lookupMap, shortform, new TreeSet<String>()); 
			_lookupMap.get(shortform).add(listcode);
			
			lcount++;
		}
		
		bread.close();
		
		_listCountMap.put(scanreq.getListCode(), lcount);
		
		// Make sure to clean up here.
		System.gc();
		
		return excount;		
	}	
	
	private TreeMap<String, Integer> loadManifest(FileSystem fsys)
	{
		TreeMap<String, Integer> manimap = Util.treemap();
		Path manipath = new Path(UserIndexUtil.getStagingManifestPath(_blockStart));
		List<String> manidata = HadoopUtil.readFileLinesE(fsys, manipath);
		
		for(String onemani : manidata)
		{
			if(onemani.trim().length() == 0)
				{ continue; }
			
			String[] lc_count = onemani.trim().split("\t");
			manimap.put(lc_count[0], Integer.valueOf(lc_count[1]));
		}
		
		return manimap;
	}
	
	public boolean stagingInfoReady(FileSystem fsys)
	{
		try {
			Path manifest = new Path(UserIndexUtil.getStagingManifestPath(_blockStart));
			return fsys.exists(manifest);
		} catch (IOException ioex) {
			throw new RuntimeException(ioex);	
		}
	}
	
	public synchronized Collection<String> lookupId(WtpId wid)
	{
		Set<String> res = _lookupMap.get(wid);
		return (res == null ? new LinkedList<String>() : res);
	}
	
	public synchronized Set<String> getListCodes()
	{
		return _listCountMap.keySet();	
	}
	
	public synchronized Map<String, Integer> getListCountMap()
	{
		return Collections.unmodifiableMap(_listCountMap);	
	}
	

}
