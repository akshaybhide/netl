
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

// Communicate with AdBoard
public class AdBoardApi
{	
	public enum AdbXmlTag { nick_nameStr, external_idStr, countryStr, created_atStr, requesterStr };
	
	private static final String ADBOARD_PULL_XML_PATH = UserIndexUtil.LOCAL_UINDEX_DIR + "/adboardapi/adboard.xml";
	
	private static final String ADB_API_TEMP_DIR = UserIndexUtil.LOCAL_UINDEX_DIR + "/adboardapi/ADB_API_TEMP";
	private static final String ADB_MASTER_LOG = UserIndexUtil.LOCAL_UINDEX_DIR + "/adboardapi/ADB_MASTER_LOG.txt";
	
	// public static final String AD_BOARD_BASE = "https://adboard.digilant.com/prod.php";
	public static final String EXTRA_STUFF = "adboardApi/aimUserIndex";
	public static final String TOKEN_STRING = "token=Sd30SEoiZMv3358DFQASFEOzoZhfa4039hdf";
	
	private static final String ID_CODE_STRING = "id";
	
	public enum CLarg { poll4new, shownew, showall, update_extid, status_update };
	
	private List<TargetList> _targetList = Util.vector();
	
	private boolean _isDev = false;
	
	
	private static PixelAccessController _PAC_SING;
	
	// Return ALL the list requests
	public Collection<TargetList> getListRequests()
	{
		Map<Integer, TargetList> reqmap = Util.treemap();
		
		for(TargetList tlist : _targetList)
		{
			int adblistid = tlist.getAdbListId();
			reqmap.put(adblistid, tlist);
			
			// This code was about finding the most recent request for a given pixel,
			// no longer used
			/*
			if(recentmap.containsKey(tlist.getPixelId()))
			{
				Util.pf("Found multiple list requests for pixel ID %d\n", tlist.getPixelId());

				TargetList other = recentmap.get(tlist.getPixelId());				
				if(other.getCreatedAt().compareTo(tlist.getCreatedAt()) < 0)
				{
					// other request comes BEFORE this one, overwrite it
					recentmap.put(tlist.getPixelId(), tlist);
				}
				
			} else {
				recentmap.put(tlist.getPixelId(), tlist);	
			}
			*/
		}

		return reqmap.values();
	}
	
	// Reads the XML file from AdBoard, uses it to populate the _targetList list
	public void readFromAdBoard() throws Exception
	{
		Document xmldoc = xmlFromUrl(getIndexRequestUrl(_isDev), ADBOARD_PULL_XML_PATH, false, "<targeting");
		
		Util.massert(xmldoc != null, "Failed to read AdBoard Xml");
		
		// ARrghhh too many lists
		NodeList nodelist =  xmldoc.getElementsByTagName("targetingList");
		
		for(int i : Util.range(nodelist.getLength()))
		{
			Node targnode = nodelist.item(i);
			TargetList targ =  TargetList.buildFromNode(targnode);
			_targetList.add(targ);
		}
	}
	
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
	
	public static class TargetList
	{
		Map<AdbXmlTag, String> _tagData = Util.treemap();
		
		private Integer _adbListId;
		
		private SortedSet<Integer> _pixelIdSet;
		
		public static TargetList buildFromNode(Node targnode)
		{
			TargetList tlist = new TargetList();
			
			for(AdbXmlTag oneatt : AdbXmlTag.values())
			{
				tlist._tagData.put(oneatt, value4SubNode(targnode, oneatt));
			}
			
			tlist._adbListId = listid4Seed(targnode);
			
			tlist._pixelIdSet = getPixelIdSet(targnode);
			
			return tlist;
		}
		
		public Integer getAdbListId()
		{ return _adbListId; }
		
		public String getExternalId()
		{ return getTagStr(AdbXmlTag.external_idStr); 	}
		
		public Set<Integer> getPixelIdSet()
		{
			return Collections.unmodifiableSet(_pixelIdSet);
			
		}
		
		// Old scheme listcode format
		public String getOldListCode()
		{
			return Util.sprintf("pixel_%d", getFirstPixelId());
		}
		
		public Integer getFirstPixelId()
		{
			return _pixelIdSet.first();
		}
			
		public String getCountryCode()
		{
			return getTagStr(AdbXmlTag.countryStr);	
		}
		
		public String getRequester()
		{
			return getTagStr(AdbXmlTag.requesterStr);	
		}		
		
		public String getNickName()
		{
			return 	Util.basicAsciiVersion(_tagData.get(AdbXmlTag.nick_nameStr));
		}		
		
		// This is a long timestamp
		public String getCreatedAt()
		{
			return 	_tagData.get(AdbXmlTag.created_atStr);
		}	
		
		// Short daycode 
		public String getCreatedAtDayCode()
		{
			Calendar cal = TimeUtil.longDayCode2Cal(getCreatedAt());
			return TimeUtil.cal2DayCode(cal);
		}
		
		public String getListCode()
		{
			String nickname = getNickName();
			
			if(nickname.startsWith("PCC_Converter_List"))
			{
				String[] toks = nickname.split("_");
				CountryCode ccode = CountryCode.valueOf(toks[3]);
				return ScanRequest.SpecpccRequest.listCode4Ctry(ccode);
			}
			
			return ScanRequest.UserRequest.getListCode(getAdbListId());
			//return Util.sprintf("pixel_%d", getPixelId());			
		}
		
		public String toString()
		{
			return Util.sprintf("TargetList adblistid=%d, nickname=%s, pixset=%s, ctry=%s, geoskew=%b, extid=%s",
				_adbListId, getNickName(), _pixelIdSet, getCountryCode(), hasGeoSkew(), getExternalId());
		}
		
		public boolean hasGeoSkew()
		{
			return "US".equals(getCountryCode());
		}
		
		private String getTagStr(AdbXmlTag xat)
		{
			Util.massert(xat.toString().endsWith("Str"), "Bad naming for String tag: %s", xat);	
			return _tagData.get(xat);
		}
		
		private Integer getTagInt(AdbXmlTag xat)
		{
			Util.massert(xat.toString().endsWith("Int"), "Bad naming for Integer tag: %s", xat);	
			return Integer.valueOf(_tagData.get(xat));
		}
	
		private static int listid4Seed(Node seednode)
		{
			Util.massert(seednode.hasAttributes(), "Seed node has no attributes");
			Node idnode = seednode.getAttributes().getNamedItem("id");
			Util.massert(idnode.getNodeType() == Node.ATTRIBUTE_NODE,
				"Incorrect node type for id node");
			
			// Util.pf("List ID is %s\n", idnode.getNodeValue());
			
			return Integer.valueOf(idnode.getNodeValue());
			
		}
		
		private static String value4SubNode(Node topnode, AdbXmlTag tagname)
		{
			String realtag = tagname.toString();
			realtag = realtag.substring(0, realtag.length()-3);
			
			NodeList subnodelist = ((Element) topnode).getElementsByTagName(realtag);
			Util.massert(subnodelist.getLength() == 1, 
				"Node list too large/small for tagname %s, found %d", tagname, subnodelist.getLength());
			
			Node singnode = subnodelist.item(0);
			// return singnode.getNodeValue();
			// recNodeInfo(singnode, 0);
			
			Util.massert(singnode.getFirstChild() != null &&
				singnode.getFirstChild().getNodeType() == Node.TEXT_NODE, "Incorrect formatting for seed node");
			
			return singnode.getFirstChild().getNodeValue();
		}
		
		private static SortedSet<Integer> getPixelIdSet(Node topnode)
		{
			SortedSet<Integer> idset = Util.treeset();
			NodeList idnodelist = ((Element) topnode).getElementsByTagName(ID_CODE_STRING);
			
			for(int i : Util.range(idnodelist.getLength()))
			{
				Node idnode = idnodelist.item(i);
				// return singnode.getNodeValue();
				// recNodeInfo(singnode, 0);
				
				Util.massert(idnode.getFirstChild() != null &&
					idnode.getFirstChild().getNodeType() == Node.TEXT_NODE, "Incorrect formatting for seed node");
				
				int oneid = Integer.valueOf(idnode.getFirstChild().getNodeValue());
				idset.add(oneid);
			}
			
			return idset;
		}
	}
	
	public static PixelAccessController getPacSing()
	{
		if(_PAC_SING == null)
			{ _PAC_SING = new PixelAccessController(); }
		
		return _PAC_SING;
	}
	
	// This code decides what level of permission to other 
	// pixel data a given list request has.
	// It is basically just a slice of AdBoard
	public static class PixelAccessController
	{
		private SortedMap<Long, Long> _pix2ClientMap = Util.treemap();
		
		public void loadData()
		{
			Util.massert(_pix2ClientMap.isEmpty(), "Only call this if map is empty");
			String sql = "SELECT id, account_id FROM adnetik.pixel WHERE account_id IS NOT NULL";
			
			List<Pair<Number, Number>> pairlist = DbUtil.execSqlQueryPair(sql, new UserIdxDb());
			
			for(Pair<Number, Number> onepair : pairlist)
			{
				_pix2ClientMap.put(onepair._1.longValue(), onepair._2.longValue());
			}			
		}
		
		public Long getClientId4Pixel(int pixelid)
		{
			if(_pix2ClientMap.isEmpty())
				{ loadData(); }
			
			long pid = (long) pixelid;
			
			// This is actually a real problem, it probably means 
			// there was a bug in the request or something
			Util.massert(_pix2ClientMap.containsKey(pid), 
				"Pixel ID %d not found in pixel->client map", pixelid);
			
			return _pix2ClientMap.get(pid);			
		}
		
		public SortedSet<Integer> getPixelAccessList(long clientid)
		{
			if(_pix2ClientMap.isEmpty())
				{ loadData(); }		
			
			SortedSet<Integer> accset = Util.treeset();
			
			for(Map.Entry<Long, Long> p2centry : _pix2ClientMap.entrySet())
			{
				if(p2centry.getValue().longValue() == clientid)
					{ accset.add(p2centry.getKey().intValue()); }
			}
			
			return accset;			
		}
		
		public int numPix()
		{
			return _pix2ClientMap.size();
		}
		
		public SortedMap<Long, Long> getPix2ClientMap()
		{
			return Collections.unmodifiableSortedMap(_pix2ClientMap);
		}
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
			
			appendLogData(adblistid, lstat);
			
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
		
		private static void appendLogData(int adblistid, AdbListStatus lstat)
		{
			String logline = Util.sprintf("%s\t%d\t%s\n", 
				SimpleMail.myTimestamp(), adblistid, lstat);
			
			try {
				// append!!!
				FileOutputStream fos = new FileOutputStream(ADB_MASTER_LOG, true);
				fos.write(logline.getBytes());
				fos.close();
			} catch (IOException ioex) {
				throw new RuntimeException(ioex);	
			}
		}
		
		// Delete temp files
		private void cleanUp()
		{
			getResponsePath(_updateId).delete();
			getStatusFilePath(_updateId).delete();
		}
		
		
		// Get the curl system call
		private String getCurlSysCall()
		{
			// curl -s -k -X POST -d @teststatus2.xml -o output123.xml https://adboard.digilant.com/adboardApi/aimUserIndex/update?token=Sd30SEoiZMv3358DFQASFEOzoZhfa4039hdf
			// -s = silent, -k = don't check SSL cert
			return Util.sprintf("curl -s -k -X POST -d @%s -o %s %s",
				getStatusFilePath(_updateId).getAbsolutePath(), getResponsePath(_updateId).getAbsolutePath(),
				getAdboardUpdateUrl());
		}
		
		// Manually write out XML data, ugh, I type with my elbows
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
	
	public static void reportUniqPixelCount(int pixid, int ucount)
	{
		// TODO
		
		
	}
	
	public static void main(String[] args) throws Exception
	{
		AdBoardApi aba = new AdBoardApi();
		aba.readFromAdBoard();	
		
		Util.pf("Found %d list requests\n", aba._targetList.size());

		for(TargetList onetarget : aba._targetList)
		{
			// Util.pf("Country is %s, nickname is %s\n", 
			//	onetarget.getCountryCode(), onetarget.getNickName());
			
			if(onetarget.getCountryCode().equals("BR"))
			{
				Util.pf("Found BR code, listcode=%s, adbid=%s, nickname is %s\n", 
					onetarget.getListCode(), onetarget.getAdbListId() ,onetarget.getNickName());
				
				
			}
			
			
		}
	}
}
