
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.ScanRequest.*;

// This class facilitates and automates the process of uploading lists to IOW,
// as well as generating the add/delete files
public class AutoUploader 
{
	private String _blockEndDay;
	
	private SimpleMail _logMail;
	private StatusReportMan _repManager = new StatusReportMan();
	
	// private FtpUtil _ftpUtil;
	private JschFtpUtil _ftpUtil;
	
	// List of requests that have been created, are on local list
	private SortedSet<PosRequest> _readySet;
	
	// List we are going to upload	
	private SortedSet<PosRequest> _toUploadSet = Util.treeset();
	
	// Lists we could upload but don't have any AdBoard/Ext line item info
	private SortedSet<PosRequest> _noExtInfoSet = Util.treeset();
	
	private static String EFF_UPLOAD_DIR;

	private static final String LIST_UPLOAD_FTP_DIR = "/wtpcookie";
	private static final String TEST_UPLOAD_FTP_DIR = "/wtpcookie/dcbtest";
	
	public static void main(String[] args) throws Exception
	{
		ArgMap amap = Util.getClArgMap(args);
		
		// Block end day for the upload
		String blockend = amap.getString("blockend",  UserIndexUtil.getCanonicalEndDaycode());
		TimeUtil.assertValidDayCode(blockend);
		Util.massert(UserIndexUtil.isBlockEndDay(blockend), "Day code %s is not a valid block-end day", blockend);
		
		String targlist = amap.getString("targlist", "all");
			
		AutoUploader autoup = new AutoUploader(blockend);
		autoup.setNonTest();
		autoup.setListTarget(targlist);
		
		if(autoup.promptCheckOkay())
		{
			autoup.processAll();
		} else {
			Util.pf("Aborting\n");
			
		}
		
		// autoup.processSingle("pixel_12023");
		// autoup.flushLogData();
	}
	
	public AutoUploader(String bd)
	{
		_blockEndDay = bd;
		
		// _ftpUtil =  FtpUtil.getIowConnection();
		try { _ftpUtil = JschFtpUtil.getIowListFtp(); }
		catch (Exception ex) {
			throw new RuntimeException(ex); 	
		}
		
		EFF_UPLOAD_DIR = TEST_UPLOAD_FTP_DIR;
		
		Util.massert(TimeUtil.checkDayCode(bd), "Invalid Day Code %s", bd);
		Util.massert(UserIndexUtil.isCanonicalDay(_blockEndDay), 
			"DayCode %s is not a canonical end day", _blockEndDay);
		
		_logMail = new DayLogMail(this, _blockEndDay);
		
		_readySet = UserIndexUtil.getReadyListSet(_blockEndDay);
		Util.massert(!_readySet.isEmpty(), "No local requests found, maybe paths have changed...?");
		_logMail.pf("Found %d local ready sets\n", _readySet.size());
	}
	
	public void setNonTest()
	{
		// _ftpUtil =  FtpUtil.getIowListConn();
		EFF_UPLOAD_DIR = LIST_UPLOAD_FTP_DIR;
	}
	
	private void setListTarget(String targcodestr)
	{
		SortedSet<PosRequest> fullset = Util.treeset();
		
		if("all".equals(targcodestr))
		{ 	
			fullset.addAll(ListInfoManager.getSing().getPosRequestSet());

		} else {
			for(String onecode : targcodestr.split(","))
			{ 
				Util.massert(ListInfoManager.getSing().havePosRequest(onecode),
					"No pos request code for %s", onecode);
				
				fullset.add(ListInfoManager.getSing().getPosRequest(onecode));
			}
		}				
					
		for(PosRequest scanreq : fullset)
		{
			if(_readySet.contains(scanreq))
			{
				// ahhh
				// if ExtListId is null, add to the noExtSet, we can't upload it
				(scanreq.getExtListId() == null ? _noExtInfoSet : _toUploadSet).add(scanreq);
			}
		}
		
		return;
	}
	
	private void processAll() throws Exception
	{
		Util.pf("Processing %d 2-upload reqs\n", _toUploadSet.size());
		
		for(PosRequest poslist : _toUploadSet)
			{ processSingle(poslist); }
		
		flushLogData();
	}
	
	private boolean promptCheckOkay()
	{
		Util.pf("Following lists have been created but are not connected to AdBoard info: \n");
		
		for(PosRequest noinforeq : _noExtInfoSet)
		{
			Util.pf("\t%s\t%s\n", Util.padstr(noinforeq.getListCode(), 30), noinforeq.getNickName());	
		}		
		
		Util.pf("The following lists are new for this week: \n");
		
		for(PosRequest good2goreq : _toUploadSet)
		{
			if(!prevLocalListReady(good2goreq))
			{ 
				Util.pf("\t%s\t%s\n", Util.padstr(good2goreq.getListCode(), 30), good2goreq.getNickName()); 
				// Util.pf("\tPrev path is %s\n",
				//	UserIndexUtil.getLocalListPath(TimeUtil.nDaysBefore(_blockEndDay, 7), good2goreq.getListCode()));
			}	
		
		
		}
		
		Util.pf("The following requests will be auto-refreshed: \n");

		for(PosRequest good2goreq : _toUploadSet)
		{
			if(prevLocalListReady(good2goreq))
				{ Util.pf("\t%s\t%s\n", Util.padstr(good2goreq.getListCode(), 30), good2goreq.getNickName());	 }
		}
		
		String promptmssg = Util.sprintf("Okay to upload above %d lists, upload dir %s",
			_toUploadSet.size(), EFF_UPLOAD_DIR);
		
		return Util.checkOkay(promptmssg);
	}
	
	private void processSingle(PosRequest poslist) throws Exception
	{
		if(!poslist.isActive())
			{ return; }	
		
		Util.massert(localListReady(poslist) && poslist.getExtListId() != null, 
			"This request %s shouldn't be on toUploadSet", poslist.getListCode());
		
		_logMail.pf("Found External List ID %s and local file for listcode %s, processing\n", poslist.getExtListId(), poslist);
		processListCode(poslist);	
	}
	
	private void flushLogData()
	{
		_logMail.send2admin();
		_repManager.flushInfo();		
	}
	
	// This is a SEPARATE check, for the sake of sanity
	private boolean localListReady(PosRequest preq)
	{
		return (new File(UserIndexUtil.getLocalListPath(_blockEndDay, preq.getListCode()))).exists();
	}
	
	private boolean prevLocalListReady(PosRequest preq)
	{
		return (new File(UserIndexUtil.getLocalListPath(getPrevBlockDay(), preq.getListCode()))).exists();
	}
	
	private void processListCode(PosRequest preq) throws Exception
	{
		Util.pf("Processing list code %s\n", preq.getListCode());
		
		File newfile = new File(UserIndexUtil.getLocalListPath(_blockEndDay, preq.getListCode()));
		File oldfile = new File(UserIndexUtil.getLocalListPath(getPrevBlockDay(), preq.getListCode()));
		
		if(!newfile.exists())
		{
			_logMail.pf("New list file not found for listcode %s/path %s skipping", 
				preq.getListCode(), newfile.getAbsolutePath());	
			return;
		}
		
		String ext_list_id = preq.getExtListId();
		Util.massert(ext_list_id != null, "Attempt to autoupload, but listcode %s has no external list id", preq.getListCode());
		
		DiffGen dgen =  new DiffGen(preq.getListCode(), _blockEndDay, ext_list_id);
		dgen.slurpNew(newfile.getAbsolutePath());
		
		if(!oldfile.exists())
			{ _logMail.pf("No previous list data found for listcode %s, using complete new list\n", preq.getListCode()); }	
		else 
			{ dgen.slurpOld(oldfile.getAbsolutePath()); }
		
		_logMail.pf("Finished slurping files, have %d new list, overlap is %d, %d old list\n", 
			dgen._newSet.size(), dgen.overlap(), dgen._oldSet.size());
		
		dgen.writeAddDelete();	
		
		doUpload(preq.getListCode(), dgen);
		
	}
	
	private void doUpload(String listcode, DiffGen dgen) throws Exception
	{
		for(boolean x : new boolean[] { true, false })
		{
			String locpath = getLocRefPath(_blockEndDay, listcode, x);
			String rempath = getRemRefPath(_blockEndDay, listcode, x);
			
			_logMail.pf("Transfering src/dst\n");
			_logMail.pf("%s\n", locpath);
			_logMail.pf("%s\n", rempath);
			
			_ftpUtil.doTransfer(locpath, rempath);
			
			Util.massert(_ftpUtil.check4File(rempath),
				"Error: file not FTP'd successfully %s", rempath);
			
			// _logMail.pf("File %s transfer successful\n", locpath);
			
			// Clean up
			// (new File(locpath)).delete();
		}
		
		_repManager.reportListRefreshUpload(listcode, 
			dgen._oldSet.size(), dgen._newSet.size(), _blockEndDay, dgen.overlap(), _logMail);		
	}
	
	
	private String getPrevBlockDay()
	{
		String prevblock = TimeUtil.nDaysBefore(_blockEndDay, UserIndexUtil.NUM_DAY_DATA_WINDOW);
		Util.massert(UserIndexUtil.isCanonicalDay(prevblock),
			"Prev block %s does not seem to be a real block start day", prevblock);
		
		return prevblock;
	}
	
	private static String getRefSimpName(String daycode, String listcode, boolean isadd)
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode %s", daycode);
		
		return Util.sprintf("listref_%s_%s_%s.tsv", listcode, (isadd ? "add" : "del"), daycode);
	}
	
	public static String getLocRefPath(String daycode, String listcode, boolean isadd)
	{		
		String simpname = getRefSimpName(daycode, listcode, isadd);
		return Util.sprintf("%s/uploadzone/%s/%s", 
			UserIndexUtil.LOCAL_UINDEX_DIR, (isadd ? "add" : "remove"), simpname);
	}
	
	public static String getRemRefPath(String daycode, String listcode, boolean isadd)
	{		
		String simpname = getRefSimpName(daycode, listcode, isadd);
		return Util.sprintf("%s/%s/%s", EFF_UPLOAD_DIR, (isadd ? "add" : "remove"), simpname);
	}
	
	public static class DiffGen
	{		
		Set<WtpId> _newSet = Util.treeset();
		Set<WtpId> _oldSet = Util.treeset();
		
		private String _dayCode; 
		private String _listCode;
				
		public DiffGen(String lc, String dc, String extid)
		{
			_listCode = lc;
			
			Util.massert(TimeUtil.checkDayCode(dc), "Invalid Day Code %s", dc);
			_dayCode = dc;
		}
		
		public void slurpNew(String newfile) throws IOException 
		{
			slurpIdSet(_newSet, newfile);
		}
		
		public void slurpOld(String oldfile) throws IOException
		{
			slurpIdSet(_oldSet, oldfile);	
		}
		
		private static void slurpIdSet(Set<WtpId> targset, String targfile) throws IOException
		{
			BufferedReader bread = FileUtils.getReader(targfile);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				WtpId wid = WtpId.getOrNull(oneline.trim());
				if(wid == null)
				{ 
					Util.pf("Warning, invalid ID %s\n", oneline);
					continue;
				}
				targset.add(wid);
			}
			bread.close();			
		}
		
		public int overlap()
		{
			int x = 0;
			
			for(WtpId id : _newSet)
				{ x += _oldSet.contains(id) ? 1 : 0; }
			
			return x;
		}
		
		public void writeAddDelete() throws IOException
		{
			{
				String addpath = getLocRefPath(_dayCode, _listCode, true);
				Set<WtpId> addset = new TreeSet<WtpId>(_newSet);	
				addset.removeAll(_oldSet);
				writeSetInfo(addset, addpath);
			}
			
			{
				String delpath = getLocRefPath(_dayCode, _listCode, false);
				Set<WtpId> delset = new TreeSet<WtpId>(_oldSet);	
				delset.removeAll(_newSet);
				writeSetInfo(delset, delpath);
			}			
		}
		
		public void writeSetInfo(Set<WtpId> srcset, String path) throws IOException
		{
			String datacenter = ListInfoManager.getSing().getDataCenterForList(_listCode);
			Util.massert(datacenter != null, "Null datacenter for listcode= %s", _listCode);
			
			long timestamp = TimeUtil.dayCode2Cal(_dayCode).getTimeInMillis();				
			
			String external_id = ListInfoManager.getSing().getExtListId(_listCode);
			Util.massert(external_id != null, "Attempt to write auto-upload for listcode %s with null ext_id", _listCode);
			
			BufferedWriter mwrite = FileUtils.getWriter(path);
			for(WtpId id : srcset)
			{
				FileUtils.writeRow(mwrite, "\t", timestamp/1000, datacenter, id, external_id, 1);
			}
			mwrite.close();
		}
	}
}
