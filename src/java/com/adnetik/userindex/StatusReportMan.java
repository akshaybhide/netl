
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

public class StatusReportMan
{
	public enum CLarg { synch, manualsetstat }
	
	
	// ListCode --> List<ReportItem>
	private Map<String, List<ReportItem>> _reportMap = Util.treemap();
	
	// List  code to current AdBoard status. this is required to 
	// make sure we never back-progress a list; if it succeeds for one week, we don't want to then
	// backtrack to "failure".
	// TreeMap<String, AdbListStatus> _adbStatusMap = null;

	public void sendReport(String listcode, ReportType rtype, String comment)
	{
		sendReport(listcode, rtype, true, comment);	
	}
	
	public void sendReport(String listcode, ReportType rtype, Boolean isokay, String comment)
	{	
		ReportItem ritem = new ReportItem(listcode, rtype, isokay, comment);

		Util.setdefault(_reportMap, listcode, new Vector<ReportItem>());
		_reportMap.get(listcode).add(ritem);
	}
	
	void sendReportPf(String listcode, ReportType rtype, Boolean isokay, String formstr, Object... varargs)
	{
		String comment = Util.sprintf(formstr, varargs);
		sendReport(listcode, rtype, isokay, comment);
	}	
	
	
	void reportCreateNew(String listcode, int adblistid, SimpleMail logmail)
	{
		sendReport(listcode, ReportType.create, true, Util.sprintf("Creation, ADB list id is %d", adblistid));
		
		sendStatus2AdboardMaybe(listcode, AdbListStatus.wait2scan, adblistid, logmail);		
	}
	
	void reportListRenew(String listcode, String newexpirdate)
	{
		TimeUtil.assertValidDayCode(newexpirdate);
		
		sendReportPf(listcode, ReportType.renew, true, 
			"List request renewed, new expiration date is %s", newexpirdate);
	}
	
	void reportListRefreshUpload(String listcode, int prvsize, int newsize, String daycode, int overlap, SimpleMail logmail)
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode %s", daycode);
		
		sendReportPf(listcode, ReportType.autorefresh, true, 
			"List refresh complete for block ending %s, prvsize=%d, newsize=%d, overlap=%d",
			daycode, prvsize, newsize, overlap);
		
		// We must have an ADB list id, otherwise we wouldn't be doing the upload
		Integer adblistid = ListInfoManager.getSing().getAdbListId(listcode);
		Util.massert(adblistid != null,
			"Failed to find an ADB list ID for code %s", listcode);
		
		sendStatus2AdboardMaybe(listcode, AdbListStatus.completed, adblistid, logmail);
	}
	
	void reportStagingCountOkay(String listcode, int numusers, String extrainfo, String daycode, SimpleMail logmail)
	{
		sendReportPf(listcode, ReportType.staging, true, 
			"Success, found %d users for staging %s, %s", numusers, daycode, extrainfo);
		
		sendStatus2AdboardMaybe(listcode, AdbListStatus.scanning, ListInfoManager.getSing().getAdbListId(listcode), logmail);
	}
	
	void reportStagingCountFail(ScanRequest scanreq, int numusers, String extrainfo, String daycode, SimpleMail logmail)
	{
		sendReportPf(scanreq.getListCode(), ReportType.staging, false, 
			"TOO FEW USERS, found %d users for staging %s, extrainfo: %s", numusers, daycode, extrainfo);
		
		sendStatus2AdboardMaybe(scanreq.getListCode(), AdbListStatus.failure, scanreq.getAdbListId(), logmail);		
	}	
	
	
	void reportLearningCountFail(String listcode, int numusers, String daycode, SimpleMail logmail)
	{
		sendReportPf(listcode, ReportType.learn, false, 
			"Found only %d users in slice data, learning aborted for daycode %s", numusers, daycode);		
	}
	
	void reportLearningSuccessful(String listcode, String daycode, SimpleMail logmail)
	{
		TimeUtil.assertValidDayCode(daycode);
		
		sendReportPf(listcode, ReportType.learn, true, 
			"Learning successful for daycode %s", daycode);
		
		sendStatus2AdboardMaybe(listcode, AdbListStatus.scoring, ListInfoManager.getSing().getAdbListId(listcode), logmail);
	}
	
	
	void reportScoreComplete(String listcode, int listsize, String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);
		
		sendReportPf(listcode, ReportType.scorecomplete, true,
			"Successfully scored %d users for week-ending %s", listsize, daycode);
	}
	
	/*
	private void writeAdbStatusMap()
	{
		// If this is null, it was never loaded in the first place, so we can just skip this
		if(_adbStatusMap == null)
			{ return; }
		
		List<String> mylist = Util.vector();
		
		for(Map.Entry<String, AdbListStatus> ent : _adbStatusMap.entrySet())
			{ mylist.add(Util.sprintf("%s\t%s", ent.getKey(), ent.getValue())); }
		
		FileUtils.writeFileLinesE(mylist, ADB_STATUS_MAP_PATH);
	}
	*/
	
	public void sendStatus2AdboardMaybe(String listcode, AdbListStatus newstat, Integer adblistid, SimpleMail logmail)
	{
		// By default, we do NOT override the complete rule
		sendStatus2AdboardMaybe(listcode, newstat, adblistid, logmail, false);
	}		
	
	public void sendStatus2AdboardMaybe(String listcode, AdbListStatus newstat, Integer adblistid, SimpleMail logmail, boolean overrideComplete)
	{
		if(adblistid == null)
			{ return; }
		
		// loadAdbStatusMap();
		// AdbListStatus curstat = _adbStatusMap.get(listcode);
		AdbListStatus curstat = getStatusInfo(listcode);
		
		// If status is "completed", we don't update
		if(overrideComplete || curstat != AdbListStatus.completed)
		{
			logmail.pf("going to update status to %s from %s for listid = %s\n",
				newstat, curstat, listcode);

			AdBoardApi.StatusUpdater statup = new AdBoardApi.StatusUpdater();
			try  { statup.sendUpdate(adblistid, newstat); }
			catch (Exception rex) { throw new RuntimeException(rex); }
			
			writeStatusInfo(listcode, newstat);
		}
	}	
	
	private static String getStatusPath(String listcode)
	{
		return Util.sprintf("%s/adboardapi/STATUS/%s.txt", UserIndexUtil.LOCAL_UINDEX_DIR, listcode); 	
	}
	
	public AdbListStatus getStatusInfo(String listcode)
	{
		String statpath = getStatusPath(listcode);
		
		if(!(new File(statpath)).exists())
			{ return AdbListStatus.mynew; }
		
		List<String> linelist = FileUtils.readFileLinesE(statpath);
		Util.massert(linelist.size() == 1, "Expected 1 line in status file, found %d for listcode %s",
			linelist.size(), listcode);
		
		return AdbListStatus.valueOf(linelist.get(0));
	}
	
	private void writeStatusInfo(String listcode, AdbListStatus newstat)
	{
		List<String> linelist = Util.vector();
		linelist.add(newstat.toString());
		FileUtils.writeFileLinesE(linelist, getStatusPath(listcode));
	}
	
	public void flushInfo()
	{
		// writeAdbStatusMap();
		
		try {
			Connection conn = (new UserIdxDb()).createConnection();
			String sql = "INSERT INTO status_info (listcode, reptype, status, tstamp, comment) VALUES (?, ?, ?, ?, ?)";
			
			for(List<ReportItem> onelist : _reportMap.values())
			{
				for(ReportItem ritem : onelist)
				{ 
					PreparedStatement pstmt = conn.prepareStatement(sql);
					pstmt.setString(1, ritem._listCode);
					pstmt.setString(2, ritem._repType.toString());
					pstmt.setString(3, (ritem._isOkay ? "OKAY" : "FAIL"));
					pstmt.setString(4, ritem._timeStamp);
					pstmt.setString(5, ritem._myComment);
					pstmt.executeUpdate();
				}
			}
			
		} catch (SQLException sqlex) {
			throw new RuntimeException(sqlex); 
		}
	}
	
	private void synchStatusInfo(String lctosynch) throws Exception
	{
		List<String> synchlist = Util.vector();

		Util.massert(ListInfoManager.getSing().havePosRequest(lctosynch),
			"Could not find listen code request %s", lctosynch);
				
		synchlist.add(lctosynch);
		
		AdBoardApi.StatusUpdater statup = new AdBoardApi.StatusUpdater();
		
		for(String listcode : synchlist)
		{
			AdbListStatus locstatus = getStatusInfo(listcode); // the local status
			
			Integer adblistid = ListInfoManager.getSing().getAdbListId(listcode);

			if(adblistid == null)
			{
				Util.pf("No ADB list ID found for listcode %s\n",
					listcode);
				continue;
			}
			
			Util.pf("Going to update listcode=%s, adbid=%d, to status=%s\n",
				listcode, adblistid, locstatus);
			
			statup.sendUpdate(adblistid, locstatus);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		CLarg myarg;
		
		try { myarg = CLarg.valueOf(args[0]); }
		catch (Exception ex) {
			
			Util.pf("Usage: StatusReportMan <synch|manualsetstat>");
			return;
		}
	
		StatusReportMan srm = new StatusReportMan();
		ArgMap amap = Util.getClArgMap(args);
		
		if(myarg == CLarg.synch)
		{
			String tosynch = amap.getString("tosynch", "all");
			
			// Reset status information
			srm.synchStatusInfo(tosynch);	
			
		} else if (myarg == CLarg.manualsetstat) {
			
			Util.massert(ListInfoManager.getSing().havePosRequest(args[1]),
				"No positive list request found with listcode %s", args[1]);
			
			PosRequest preq = ListInfoManager.getSing().getPosRequest(args[1]);
			AdbListStatus newstat = AdbListStatus.valueOf(args[2]);
			
			Util.pf("Going to manually override request for listcode=%s, adblistid=%s, extlistid=%s\n",
				preq.getListCode(), preq.getAdbListId(), preq.getExtListId());
			
			Util.pf("Current status is %s, new status will be %s\n",
				srm.getStatusInfo(preq.getListCode()), newstat);
				
			if(Util.checkOkay("Okay to proceed?"))
			{
				
				srm.sendReportPf(preq.getListCode(), ReportType.m_override, true, 
					"Manual override of status from %s to %s", 
					srm.getStatusInfo(preq.getListCode()), newstat);				
				
				srm.sendStatus2AdboardMaybe(preq.getListCode(), newstat, preq.getAdbListId(), new SimpleMail("GIMP"), true);
				
				srm.flushInfo();
			}
				
			// public void sendStatus2AdboardMaybe(String listcode, AdbListStatus newstat, Integer adblistid, SimpleMail logmail, boolean overrideComplete)
		}
	}
	
	private static class ReportItem
	{
		private String _listCode;
		private ReportType _repType;
		private Boolean _isOkay;
		private String _timeStamp;		
		private String _myComment;
		
		public ReportItem(String lcode, ReportType rt, Boolean isokay, String comment)
		{
			_listCode = lcode;
			_repType = rt;
			_isOkay = isokay;
			_myComment = comment;
			_timeStamp = TimeUtil.longDayCodeNow();
		}		
	}
	
}

