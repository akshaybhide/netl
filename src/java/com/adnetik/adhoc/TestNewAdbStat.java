package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

// import com.sun.org.apache.xerces.internal.parsers.DOMParser;
import org.apache.xerces.parsers.DOMParser;
import org.w3c.dom.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.analytics.*;	
import com.adnetik.userindex.*;	
import com.adnetik.userindex.UserIndexUtil.*;

// Use to rebuild a set of HDFS gzip files that are corrupted on the tails.
public class TestNewAdbStat
{
	/*
	public enum AdbXmlTag { nick_nameStr, idInt, external_idStr, countryStr, created_atStr };
	
	private static final String ADBOARD_PULL_XML_PATH = "/home/burfoot/userindex/adboardapi/adboard.xml";
	
	private static final String ADB_API_TEMP_DIR = "/home/burfoot/userindex/adboardapi/ADB_API_TEMP";
	
	// public static final String AD_BOARD_BASE = "https://adboard.digilant.com/prod.php";
	public static final String EXTRA_STUFF = "adboardApi/aimUserIndex";
	public static final String TOKEN_STRING = "token=Sd30SEoiZMv3358DFQASFEOzoZhfa4039hdf";
	
	public enum CLarg { poll4new, shownew, showall, update_extid, status_update };
		
	private boolean _isDev = false;

	
	private static String getAdBoardBaseUrl(boolean isdev)
	{
		return Util.sprintf("https://adboard%s.digilant.com", (isdev ? "-dev" : ""));	
	}	
	
	public static String getIndexRequestUrl(boolean isdev)
	{
		return Util.sprintf("%s/%s/index?%s", getAdBoardBaseUrl(isdev), EXTRA_STUFF, TOKEN_STRING);
	}

	private static Document xmlFromUrl(String httpurl, String savepath, boolean delete) throws Exception
	{
		return xmlFromUrl(httpurl, savepath, delete, null);	
	}

	private static Document xmlFromUrl(String httpurl, String savepath, boolean delete, String splitstr) throws Exception
	{
		// This is kind of a hack - pull the XML file from AdBoard, write it to the disk
		List<String> xmldata = Util.httpDownload(httpurl);
		
		if(splitstr != null)
		{
			String xmlstr = Util.join(xmldata, "");
			
			xmldata.clear();
			
			String[] toks = xmlstr.split(splitstr);
			Util.pf("Found %d tokens for xmlstr, split on %s, length is %d\n", 
				toks.length, splitstr, xmlstr.length());
			
			// Have to add back on the splitstr, or else it will be removed and the XML will be invalid
			xmldata.add(toks[0]);
			for(int i = 1; i < toks.length; i++)
				{ xmldata.add(splitstr + toks[i]); }
		} 
		
		FileUtils.writeFileLinesE(xmldata, savepath);
		Util.pf("Wrote %d lines to path %s\n", xmldata.size(), savepath);
		
		DOMParser parser = new DOMParser();
		parser.parse(savepath);
		Document xmldoc = parser.getDocument();
		
		if(delete)
			{ (new File(savepath)).delete(); }
		
		return xmldoc;
	}
		
	
	public static class StatusUpdater
	{
		private boolean isdev = false;
		
		private Long _updateId;
		
		public StatusUpdater()
		{
			_updateId = getUpdateId();
		} 
		
		private static Long getUpdateId()
		{
			Random jr = new Random();
			
			for(int i = 0; i < 10; i++)
			{
				long upid = jr.nextLong();
				upid = (upid < 0 ? -upid : upid);
				
				if(!getResponsePath(upid).exists())
				{
					Util.massert(!getStatusFilePath(upid).exists(),
						"Response path not present, but status file is for upid=%d", upid);
					
					return upid;
				}
			}
			
			throw new RuntimeException("Unable to get temp ID after 10 tries, clean out ABD_API_DIR!!!");			
		}
		
		private static File getResponsePath(long upid)
		{
			return getAdbTempFile("adbresponse", upid);
		}
		
		private static File getStatusFilePath(long upid)
		{
			return getAdbTempFile("statusupload", upid);
			
		}		
		
		private static File getAdbTempFile(String prefcode, long upid)
		{
			String mypath = Util.sprintf("%s/%s__%d.xml", ADB_API_TEMP_DIR, prefcode, upid);
			return new File(mypath);
		}
		
		private String getAdboardUpdateUrl()
		{
			return Util.sprintf("%s/%s/update?token=Sd30SEoiZMv3358DFQASFEOzoZhfa4039hdf",
				getAdBoardBaseUrl(isdev), EXTRA_STUFF);
		}
		
		void sendUpdate(int adblistid, AdbListStatus lstat) throws Exception
		{	
			writeStatusUploadFile(adblistid, lstat);
			
			String curlcall = getCurlSysCall();
			
			// Util.pf("Curl call is:\n%s\n", curlcall);
			
			List<String> outlines = Util.vector(); List<String> errlines = Util.vector();
			Util.syscall(curlcall, outlines, errlines);
			
			Util.massert(outlines.size() + errlines.size() == 0, 
				"Found output %s or error %s lines from CURL", outlines, errlines);
		
			checkResponseFile();
			
			cleanUp();
		}
		
		private void checkResponseFile()
		{
			List<String> resplist = FileUtils.readFileLinesE(getResponsePath(_updateId).getAbsolutePath());
			String fulltext = Util.join(resplist, "");
			
			// This means the response contains something other than success
			Util.massert(fulltext.indexOf("<success>1</success>") > -1, 
				"Could not find success in response %s", fulltext);
		}
		
		// Delete temp files
		private void cleanUp()
		{
			getResponsePath(_updateId).delete();
			getStatusFilePath(_updateId).delete();
		}
		
		private String getCurlSysCall()
		{
			// curl -s -k -X POST -d @teststatus2.xml -o output123.xml https://adboard.digilant.com/adboardApi/aimUserIndex/update?token=Sd30SEoiZMv3358DFQASFEOzoZhfa4039hdf
			// -s = silent, -k = don't check SSL cert
			return Util.sprintf("curl -s -k -X POST -d @%s -o %s %s",
				getStatusFilePath(_updateId).getAbsolutePath(), getResponsePath(_updateId).getAbsolutePath(),
				getAdboardUpdateUrl());
		}
		
		private void writeStatusUploadFile(int adblistid, AdbListStatus lstat)
		{
			List<String> flist = Util.vector();
			flist.add("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
			flist.add("<targetingList>");
			flist.add(Util.sprintf("<listId>%d</listId>", adblistid));
			flist.add(Util.sprintf("<status>%d</status>", lstat.getAdboardCode()));
			flist.add("</targetingList>");
			
			FileUtils.writeFileLinesE(flist, getStatusFilePath(_updateId).getAbsolutePath());
		}
	}	
	*/
	
	/*
	public static void main(String[] args) throws Exception
	{
		
		Integer adblistid = Integer.valueOf(args[0]);
		AdbListStatus newstat = AdbListStatus.valueOf(args[1]);
		
		Util.pf("Going to update for ADB ID %d, status=%s\n",
			adblistid, newstat);
		
		AdBoardApi.StatusUpdater statup = new AdBoardApi.StatusUpdater();
		statup.sendUpdate(adblistid, newstat);
	}
	*/
}
