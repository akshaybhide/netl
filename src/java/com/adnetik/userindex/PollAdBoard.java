
package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.sql.*;
import java.net.*;

// import com.sun.org.apache.xerces.internal.parsers.DOMParser;
import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

import com.adnetik.shared.*;

import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.AdBoardApi.*;
import com.adnetik.userindex.ScanRequest.*;

// This code uses AdBoardApi to check on new requests, and show information about new requests
// This code was originally in AdBoardApi, but this caused cyclicality problems:
// the poll code needs to use StatusReportMan, which itself calls AdBoardApi to send status update requests.
// So now instead PollAdBoard -> StatusReportMan -> AdboardApi
public class PollAdBoard
{	
	public enum CLarg { poll4new, shownew, showall, update_extid, status_update, dateinfo };
				
	public static void main(String[] args) throws Exception
	{	
		CLarg myarg; 
		try { myarg = CLarg.valueOf(args[0]); }
		catch (Exception ex) {
			Util.pf("Usage PollAdBoard <argcode>\n");
			return;
		}
		// Util.pf("Going to read from %s\n", getIndexRequestUrl());
				
		SimpleMail logmail = new SimpleMail("PollAdBoardReport option=" + myarg);
		
		StatusReportMan reportman = new StatusReportMan();

		if(myarg == CLarg.status_update)
		{
			int targid = Integer.valueOf(args[1]);
			AdbListStatus argstat = AdbListStatus.valueOf(args[2]);
			
			Util.pf("Going to update status of target list %d to %s\n", targid, argstat);
			StatusUpdater statup = new StatusUpdater();
			statup.sendUpdate(targid, argstat);
			return;
		}
 
		AdBoardApi aba = new AdBoardApi();
		aba.readFromAdBoard();		
		
		if(myarg == CLarg.poll4new)
		{
			int newcount = 0;
			
			for(TargetList listreq : aba.getListRequests())
			{
				String listcode = listreq.getListCode();
				
				if(!ListInfoManager.getSing().havePosRequest(listcode))
				{
					// This is a NEW request
					UserRequest ureq = new UserRequest(listcode);
					
					// These initial pieces of data are those that CANNOT be altered at
					// a later stage - or it requires admin action to do so.
					ureq.setInitialData(CountryCode.valueOf(listreq.getCountryCode()), listreq.hasGeoSkew());
					ListInfoManager.getSing().insertListCodeShell(ureq);
					
					ureq.setUpdatePixelSet(listreq.getPixelIdSet());

					logmail.pf("Creating NEW list request for listcode %s\n", listcode);
					reportman.reportCreateNew(listcode, listreq.getAdbListId(), logmail); 
					newcount++;
				}
			}
								
			for(TargetList listreq : aba.getListRequests())	
			{
				String listcode = listreq.getListCode();
				
				// Here we POTENTIALLY update the request 
				// we are guaranteed that this will come back with a valid posrequest
				PosRequest preq = ListInfoManager.getSing().getPosRequest(listcode);
				
				// Special behavior for Special PCC request, just update the AdbListId, and ExtListId
				if(preq instanceof SpecpccRequest)
				{
					setMaybeLog(preq, listreq.getAdbListId(), LCodeInt.adb_list_id, logmail);					
					setMaybeLog(preq, listreq.getExternalId(), LCodeStr.ext_list_id, logmail);					
					
					if(preq.isModified())
					{
						logmail.pf("Saving modifications to listcode %s\n", listcode);
						preq.persist2db(new UserIdxDb());
					}				
					
					continue;
				}
				
				if(!(preq instanceof UserRequest))
				{
					logmail.pf("WARNING: listcode %s does not correspond to a UserRequest object", listcode);
					logmail.pf("Found a %s instead", preq);
					continue;
				}
				
				UserRequest ureq = (UserRequest) preq;
				setMaybeLog(ureq, listreq.getAdbListId(), LCodeInt.adb_list_id, logmail);
				
				setMaybeLog(ureq, listreq.getExternalId(), LCodeStr.ext_list_id, logmail);
				setMaybeLog(ureq, listreq.getNickName(), LCodeStr.nickname, logmail);
				setMaybeLog(ureq, listreq.getRequester().toLowerCase(), LCodeStr.requester, logmail);
				setMaybeLog(ureq, listreq.getOldListCode(), LCodeStr.old_listcode, logmail);
				
				{
					Set<Integer> prevset = ureq.getPixSet();
					boolean ischange = ureq.setUpdatePixelSet(listreq.getPixelIdSet());
					
					if(ischange)
					{
						logmail.pf("Updated pixel set for listcode %s, old=%s, new=%s\n",
							listcode, prevset, ureq.getPixSet());
					}
				}
				
				// TODO 
				if(listreq.getCreatedAtDayCode().compareTo(ureq.getEntryDate()) > 0)
				{
					
				}
				
				// preq.setEntryDate(listreq.getCreatedAtDayCode());
				// TODO: Currently not picking this up from the XML, but maybe we will at some point
				// Okay, until we pick this up in XML, this is not the right place to do this
				// preq.setTargSizeK(ListInfoManager.getSing().getDefaultListSizeK(listreq.getCountryCode()));
								
				// TODO save a "valid as of" field
				
				if(ureq.isModified())
				{
					logmail.pf("Saving modifications to listcode %s\n", listcode);
					ureq.persist2db(new UserIdxDb());
				}
			}
			
			logmail.send2admin();
			reportman.flushInfo();
		}
		
		if(myarg == CLarg.dateinfo)
		{
			for(TargetList listreq : aba.getListRequests())
			{
				// Util.pf("List req %s came in at %s\n", listreq, listreq.getCreatedAt());
				
				
				
			}
		}
		
		if(myarg.toString().startsWith("show"))
		{
			boolean newonly = (myarg == CLarg.shownew);
			
			/*
			Set<String> listcodeset = ListInfoManager.getSing().getFullListCodeSet();
						
			if(newonly)
			{
				for(TargetList listreq : aba.getNewListRequests())
				{
					Util.pf("Found NEW list request %s\n", listreq);
				}
								
			} else {
				for(TargetList listreq : aba.getListRequests())
				{	
					Util.pf("Found list request %s\n", listreq);
				}
			}
			*/
		}
	}
	
	private static void setMaybeLog(PosRequest ureq, String newval, ScanRequest.LCodeStr fieldcode, SimpleMail logmail)
	{
		String oldval = ureq.getStrValue(fieldcode);
		
		boolean ischange = ureq.setStrValue(fieldcode, newval);
		
		if(ischange)
		{
			logmail.pf("Update for listcode %s, field %s :: %s --> %s\n",
				ureq.getListCode(), fieldcode, oldval, newval);
		}
	}
	
	private static void setMaybeLog(PosRequest ureq, Integer newval, ScanRequest.LCodeInt fieldcode, SimpleMail logmail)
	{
		Integer oldval = ureq.getIntValue(fieldcode);
		
		boolean ischange = ureq.setIntValue(fieldcode, newval);
		
		if(ischange)
		{
			logmail.pf("Update for listcode %s, field %s :: %d --> %d\n",
				ureq.getListCode(), fieldcode, oldval, newval);
		}
	}	
	
	
}
