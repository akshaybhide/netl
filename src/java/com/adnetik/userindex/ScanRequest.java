
package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.sql.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.ExcName;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.DbUtil.*;

import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.data_management.ParamPixelMan;

// Object representing a single ScanRequest.
public abstract class ScanRequest implements Comparable<ScanRequest>, Serializable
{	
	StagingType _stageType;
	
	// These must match 
	enum LCodeInt { geoskew, adb_list_id, size_in_k  }
	
	enum LCodeStr { listcode, nickname, country, entry_date, ext_list_id, requester, old_listcode }
	
	private ScanRequest(StagingType stype)
	{
		_stageType = stype;
	}
	
	public int compareTo(ScanRequest that)
	{
		return getListCode().compareTo(that.getListCode());	
	}
	
	public boolean equals(ScanRequest that)
	{
		return getListCode().equals(that.getListCode());	
	}
	
	@Override
	public int hashCode() 
	{
		return getListCode().hashCode();	
	}
	
	private static Map<CountryCode, NegRequest> _NEG_REQ_MAP;	
	
	// This is the LISTCODE
	public abstract String getListCode();
	
	public abstract String getNickName();
	
	public abstract String getEntryDate();
	
	// This is basically the pixel(s) and the list request type
	public abstract String getBasicInfo();
	
	// PosRequest overrides with actual value in database
	public String getOldListCode()
	{
		return getListCode();	
	}
	
	// By default, list requests do not expire. Subclasses override
	// to provide expiration date logic
	public String getExpirationDate() { return "2100-01-01"; }
	
	// Subclasses override
	public Integer getAdbListId() { return null; }
	
	public abstract CountryCode getCountryCode();
	
	public abstract boolean hasGeoSkew();
	
	public boolean isActive()
	{
		return isActiveOn(TimeUtil.getTodayCode());
	}
	
	public boolean isActiveOn(String daycode)
	{
		TimeUtil.assertValidDayCode(daycode);
		return getExpirationDate().compareTo(daycode) >= 0;
	}	
		
	public StagingType getStageType()
	{
		return _stageType;
	}	
	
	public interface HasClientPixel 
	{
		public abstract Integer getAllowedPixel();	
	}
	
	public static ScanRequest buildFromListCode(String listcode)
	{
		if(listcode.startsWith(StagingType.pixel.toString()))
			{ return new PixelRequest(listcode); }
		
		if(listcode.startsWith(StagingType.pxprm.toString()))
			{ return new PxprmRequest(listcode); }
		
		if(listcode.startsWith(StagingType.user.toString()))
			{ return new UserRequest(listcode); }

		if(listcode.startsWith(StagingType.sysmulti.toString()))
			{ return new SysMultiRequest(listcode); }		
		
		if(listcode.startsWith(StagingType.negative.toString()))
		{ 
			// eg negative_US_000
			String[] toks = listcode.split("_");
			return new NegRequest(CountryCode.valueOf(toks[1]), Integer.valueOf(toks[2]));
		}
		
		if(listcode.startsWith(StagingType.specpcc.toString()))
			{ return new SpecpccRequest(listcode); }
		
		Util.massert(false, "Unknown prefix for list code, must be one of %s, found %s",
			Util.join(StagingType.values(), ","), listcode);
		
		// Never reach here
		return null;
	}
	
	static Map<CountryCode, NegRequest> getNegReqMap()
	{
		if(_NEG_REQ_MAP == null)
		{
			_NEG_REQ_MAP = Util.treemap();
			for(CountryCode ccode : UserIndexUtil.COUNTRY_CODES)
			{
				NegRequest nreq = new NegRequest(ccode, 0);
				_NEG_REQ_MAP.put(ccode, nreq);
			}
		}
		
		return Collections.unmodifiableMap(_NEG_REQ_MAP);
	}
	
	static List<ScanRequest> grabFromDB(ConnectionSource csrc)
	{
		Map<String, ScanRequest> reqmap = Util.treemap();
		Connection conn = null;
		
		try { 
			
			conn = csrc.createConnection();	
			
			PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM listen_code");
			ResultSet rset = pstmt.executeQuery();
			
			while(rset.next())
			{
				String listcode = rset.getString("listcode");
				ScanRequest scanr = buildFromListCode(listcode);
				
				if(!(scanr instanceof PosRequest))
					{ continue; }
								
				((PosRequest) scanr).populateFromRowSet(rset);
				
				Util.massert(scanr.getListCode().equals(listcode),
					"ScanReq getListCode() is %s but table listcode is %s",
					scanr.getListCode(), listcode);
				
				reqmap.put(scanr.getListCode(), scanr);
			}
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
			
		} finally { 
			
			try { 
				if(conn != null)
					{ conn.close(); }
			} catch (SQLException sqlex) { }
		}
		
		// Populate multipix requests
		{
			String sql = "SELECT listcode, pixid FROM multipix";
			List<Pair<String, Integer>> multipixlist = DbUtil.execSqlQueryPair(sql, csrc);
	
			for(Pair<String, Integer> onepair : multipixlist)
			{
				ScanRequest scanreq = reqmap.get(onepair._1);
				
				// Should be protected here because of foreign key constraint
				Util.massert(scanreq != null,
					"Listcode %s not found in reqmap", onepair._1);
				
				// Some chance we could put a non-multipix listcode in the multipix tabl
				Util.massert(scanreq instanceof MultiPixRequest,
					"Listcode %s is not a MultiPixRequest", onepair._1);
				
				((MultiPixRequest) scanreq).addPixId(onepair._2);
			}
		}
		
		return new Vector<ScanRequest>(reqmap.values());
	}
	
	
	// This is a "positive" request, as opposed to a negative request 
	public abstract static class PosRequest extends ScanRequest
	{
		private Map<LCodeInt, Integer> _intMap = Util.treemap();
		private Map<LCodeStr, String> _strMap = Util.treemap();
		
		protected boolean _hasMod;
		
		private PosRequest(StagingType stype)
		{
			super(stype);
			
			Util.massert(stype.isPositive(), "Invalid staging type for positive request %s", stype);
		}
		
		void setInitialData(CountryCode ccode, boolean hasgeoskew)
		{
			_hasMod = true;
			
			Util.massert(_intMap.isEmpty() && _strMap.isEmpty(),
				"setInitialData(..) can only be called before any other pieces of info are set");
			
			_strMap.put(LCodeStr.country, ccode.toString());
			_intMap.put(LCodeInt.geoskew, (hasgeoskew ? 1 : 0));
			_strMap.put(LCodeStr.entry_date, TimeUtil.getTodayCode());	
			
			// TODO: add active_as_of here
		}
		
		public NegRequest getNegRequest()
		{
			NegRequest negreq = getNegReqMap().get(getCountryCode());
			
			Util.massert(negreq != null,
				"No negative request found for country %s, listcode %s",
				getCountryCode(), getListCode());
			
			return negreq;
		}		
		
		@Override
		public String getOldListCode()
		{
			return _strMap.get(LCodeStr.old_listcode);	
		}	
		
		public void setOldListCode(String oldcode)
		{
			setStrValue(LCodeStr.old_listcode, oldcode);
		}
		
		// Subclasses override
		protected void persistExtraData(Connection conn) throws SQLException {}
		
		private void populateFromRowSet(ResultSet rset) throws SQLException
		{
			for(LCodeStr strcode : LCodeStr.values())
			{
				String val = rset.getString(strcode.toString());
				
				if((strcode == LCodeStr.ext_list_id || strcode == LCodeStr.requester) && val == null)
					{ continue; }
				
				Util.massert(val != null, "Found null value for field %s", strcode);
				
				// dates must be valid
				if(strcode.toString().endsWith("date"))
					{ TimeUtil.assertValidDayCode(val); }	
				
				_strMap.put(strcode, val);
			}
			
			for(LCodeInt intcode : LCodeInt.values())
			{
				String val = rset.getString(intcode.toString());
				
				if(val != null)
					{ _intMap.put(intcode, Integer.valueOf(val)); }
			}
		}
		
		// Save the request to the database. The listcode record must already exist!!!
		public void persist2db(ConnectionSource csrc)
		{
			try { 
				Connection conn = csrc.createConnection();	
				
				// Okay, this technique ensures that we are okay even 
				// if some of these values are null
				for(LCodeStr lcode : _strMap.keySet())
					{ sendVal2Db(conn, lcode); }
				
				for(LCodeInt lcode : _intMap.keySet())
					{ sendVal2Db(conn, lcode); }
				
				persistExtraData(conn);
				
				conn.close();
				
			} catch (SQLException sqlex) {
				
				throw new RuntimeException(sqlex);
			}
			
			_hasMod = false;
		}
		
		// This needs to be preparedstatements because we need to deal with character encoding funkiness maybe
		private void sendVal2Db(Connection conn, Object lcode) throws SQLException
		{
			Util.massert(lcode instanceof LCodeInt || lcode instanceof LCodeStr,
				"Must call with LCodeInt or LCodeStr, used a %s", lcode);
			
			PreparedStatement pstmt = conn.prepareStatement(Util.sprintf("UPDATE listen_code SET %s = ? WHERE listcode = ?", lcode.toString()));
			
			if(lcode instanceof LCodeStr)
				{ pstmt.setString(1, _strMap.get(lcode)); }
			else // instanceof LCodeINt
				{ pstmt.setInt(1, _intMap.get(lcode)); }

			pstmt.setString(2, getListCode());
			int rows = pstmt.executeUpdate();
			pstmt.close();
			
			Util.massert(rows == 1, "Expected to update exactly one row, instead got %d, record must exist before calling this method", rows);			
		}	
		
		// Can be null if the ADB_LIST_ID is not set in the database
		@Override
		public Integer getAdbListId()
		{
			return _intMap.get(LCodeInt.adb_list_id);
		}
		
		public String getExtListId()
		{
			return _strMap.get(LCodeStr.ext_list_id);	
		}
		
		public String getRequester()
		{
			return _strMap.get(LCodeStr.requester);	
		}		
		
		boolean setHasGeoSkew(boolean geoskew)
		{
			return setIntValue(LCodeInt.geoskew, (geoskew ? 1 : 0));
		}
		
		boolean setNickName(String newnick)
		{
			return setStrValue(LCodeStr.nickname, newnick);	
		}
		
		boolean setAdbListId(int adbid)
		{
			return setIntValue(LCodeInt.adb_list_id, adbid);	
		}
		
		boolean setExtListId(String extlistid)
		{
			return setStrValue(LCodeStr.ext_list_id, extlistid);
		}
		
		boolean setTargSizeK(int sizeink)
		{
			return setIntValue(LCodeInt.size_in_k, sizeink);
		}
		
		boolean setRequester(String requester)
		{
			return setStrValue(LCodeStr.requester, requester);
		}
		
		Integer getIntValue(LCodeInt lcode)
		{
			return _intMap.get(lcode);	
		}
		
		String getStrValue(LCodeStr lcode)
		{
			return _strMap.get(lcode);	
		}
		
		boolean setStrValue(LCodeStr lcode, String newval)
		{
			if(newval.equals(_strMap.get(lcode)))
				{ return false; }
			
			_strMap.put(lcode, newval);
			_hasMod = true;		
			return true;
		}
		
		boolean setIntValue(LCodeInt lcode, Integer newval)
		{
			if(newval.equals(_intMap.get(lcode)))
				{ return false; }
			
			_intMap.put(lcode, newval);
			_hasMod = true;		
			return true;
		}	
		
		public boolean hasGeoSkew()
		{
			return _intMap.get(LCodeInt.geoskew) == 1;
		}
		
		public Integer getTargSizeK()
		{
			return _intMap.get(LCodeInt.size_in_k); 
		}	
		
		@Override
		public String getNickName()
		{
			return _strMap.get(LCodeStr.nickname);	
		}
		
		@Override
		public String getEntryDate()
		{
			return _strMap.get(LCodeStr.entry_date);	
		}
		
		// TODO: this should really be called "active_as_of"
		// and there should be a separate thing for "created_on"
		boolean setEntryDate(String daycode)
		{
			TimeUtil.assertValidDayCode(daycode);
			return setStrValue(LCodeStr.entry_date, daycode);
		}		
		
		@Override
		public String getExpirationDate()
		{
			return TimeUtil.nDaysAfter(getEntryDate(), 105);
		}				
		
		@Override
		public CountryCode getCountryCode()
		{
			return CountryCode.valueOf(_strMap.get(LCodeStr.country));
		}
		
		public String toString()
		{
			return Util.sprintf("STR %s, INT %s", _strMap, _intMap);	
		}
		
		public boolean isModified()
		{
			return _hasMod;
		}
		

	}
	

	
	// Parameterized pixel request
	public static class PxprmRequest extends PosRequest implements HasClientPixel
	{
		private int _pixelId;
		
		private char _reqChar = 'A';
		
		private SortedSet<Pair<String, String>> _kvQuerySet = Util.treeset();
		
		public PxprmRequest(String lc)
		{
			super(StagingType.pxprm);
			
			String[] toks = lc.toString().split("_");
			
			Util.massert(toks.length == 3 && toks[2].length() == 1, 
				"Badly formatted pxprm list code %s", lc);
			
			_pixelId = Integer.valueOf(toks[1]);
			_reqChar = toks[2].charAt(0);
			
			loadQueryInfo();
		}
		
		public String getListCode()
		{
			return Util.varjoin("_", StagingType.pxprm, 
				Util.padLeadingZeros(_pixelId, 7), _reqChar);
		}
		
		public int getPixelId()
		{
			return _pixelId;	
		}
		
		public StagingType getStageType()
		{
			return StagingType.pxprm;	
		}	
		
		private void loadQueryInfo()
		{
			List<Pair<String, String>> pairlist = UserIndexUtil.getPixParamList(this);
			_kvQuerySet.addAll(pairlist);
		}
		
		public String getKeyValString()
		{
			// Could do something prettier here
			return Util.join(_kvQuerySet, "__");
		}
		
		@Override
		public String getBasicInfo()
		{
			return Util.sprintf("Pixel-Param request: %s", getKeyValString());
		}
		
		// This does NOT cache the result
		// If it did, we could end up blowing out memory, since the ScanRequest 
		// objects ARE kept around
		public TreeSet<WtpId> grabIdSet()
		{
			TreeSet<WtpId> idset = Util.treeset();
			ParamPixelMan.QueryRunner qrun = new ParamPixelMan.QueryRunner(_pixelId);
			
			for(Pair<String, String> keyval : _kvQuerySet)
			{
				Util.pf("Running PXPRM query for key=%s, val=%s... ", keyval._1, keyval._2);
				
				Set<WtpId> oneset = qrun.get4KeyVal(keyval._1, keyval._2);
				idset.addAll(oneset);
				
				Util.pf(" ... done, found %d uniques, new total is %d\n",
					oneset.size(), idset.size());
			}
			
			return idset;
		}
		
		public Integer getAllowedPixel()
		{
			return _pixelId;	
		}
	}
	
	// Special PCC lists
	public static class SpecpccRequest extends PosRequest
	{
		private CountryCode _ctryCode;
			
		SpecpccRequest(String listcode)
		{
			super(StagingType.specpcc);
			
			String[] toks = listcode.split("_");
			Util.massert(toks.length == 2, 
				"Bad listcode for specpcc request %s", listcode);
			
			_ctryCode = CountryCode.valueOf(toks[1]);
		}
		
		public String getListCode()
		{
			return listCode4Ctry(_ctryCode);
		}
		
		public static String listCode4Ctry(CountryCode ccode)
		{
			return Util.varjoin("_", StagingType.specpcc, ccode.toString().toUpperCase());
			
		}
	
		// Ugh, back-override to make this eternal
		@Override
		public String getExpirationDate()
		{
			return "2100-01-01";
		}
		
		@Override
		public CountryCode getCountryCode()
		{
			CountryCode cc = super.getCountryCode();
			
			Util.massert(cc == _ctryCode, "Found %s in database but listcode is %s",
				cc, _ctryCode);
			
			return cc;
		}
		
		@Override
		public String getBasicInfo()
		{
			return Util.sprintf("Special PCC List for %s", getCountryCode());	
		}
		
		// remove this
		// Okay, this is not so easy to do ... be careful...
		public String getOldCode()
		{
			return Util.sprintf("special_PCC_%s", getCountryCode().toString().toUpperCase());
		}
	}		
	
	// Multipixel request
	public static abstract class MultiPixRequest extends PosRequest
	{
		protected SortedSet<Integer> _pixSet = Util.treeset();
		
			
		private MultiPixRequest(StagingType subtype)
		{
			super(subtype);
			
			Util.massert(subtype.isMulti(),
				"Only allowed to build MultiPix with USRMULTI");
		}
		
		public Set<Integer> getPixSet()
		{
			return Collections.unmodifiableSet(_pixSet);	
		}
		
		// This is really more like private
		protected void addPixId(int pixid)
		{ 
			Util.massert(!_pixSet.contains(pixid),
				"Attempt to add pixel ID %d to pixset %s for scanreq %s",
				pixid, _pixSet, getListCode());
			
			_pixSet.add(pixid); 
			
			// Util.pf("Adding pixid %d to scanreq %s\n",
			//	pixid, getListCode());
		}
			
		protected void removePixId(int pixid)
		{
			Util.massert(_pixSet.contains(pixid), 
				"Attempt to remove pixel %d not in pixset %s", 
				pixid, _pixSet);
			
			Util.pf("Removing pixel %d from pixset %s for %s\n",
				pixid, _pixSet, getListCode());			
			
			_pixSet.remove(pixid);
		}
		
		// This method sets the pixel set to the given collection
		protected boolean setUpdatePixelSet(Collection<Integer> priorcol)
		{
			boolean ischange = false;
			Set<Integer> defset = Util.treeset();
			defset.addAll(priorcol);
			
			for(int pixid : defset)
			{
				if(!_pixSet.contains(pixid))
				{ 
					addPixId(pixid); 
					_hasMod = true;
					ischange = true;
				}
			}
			
			for(int pixid : _pixSet)
			{
				if(!defset.contains(pixid))
				{ 
					removePixId(pixid); 
					_hasMod = true;
					ischange = true;					
				}
			}
			
			return ischange;
		}
		
		protected void persistExtraData(Connection conn) throws SQLException
		{
			{
				String delsql = Util.sprintf("DELETE FROM multipix WHERE listcode = '%s'", getListCode());
				DbUtil.execSqlUpdate(delsql, conn);
			} 
			
			for(Integer pixid : _pixSet)
			{
				String insql = Util.sprintf("INSERT INTO multipix VALUES ('%s', %d)", getListCode(), pixid);
				DbUtil.execSqlUpdate(insql, conn);							
			}
			
			// Util.pf("Saved pixel set %s for scanreq %s\n",
			//	_pixSet, getListCode());
		}
		
		@Override
		public String getBasicInfo()
		{
			return Util.sprintf("Request for pixel(s): %s", getPixSet());	
		}		
	}		
	
	// This is the basic pixel request
	public static class PixelRequest extends MultiPixRequest implements HasClientPixel
	{
		private int _pixelId;
		
		private PixelRequest(String lc)
		{
			super(StagingType.pixel);	
			
			String[] toks = lc.toString().split("_");
			
			Util.massert(toks.length == 2, "Badly formatted pixel list code %s", lc);
			_pixelId = Integer.valueOf(toks[1]);
		}
		
		PixelRequest(int pixid)
		{
			this("pixel_" + pixid);
		}
		
		public String getListCode()
		{
			return Util.varjoin("_", StagingType.pixel, _pixelId);	
		}
		
		public int getPixelId()
		{
			return _pixelId;	
		}
		
		public String toString()
		{
			return Util.sprintf("PixelRequest pixid=%d\n", _pixelId);
			
		}
		
		@Override
		public void addPixId(int pixid)
		{
			Util.massert(pixid == _pixelId, 
				"Attempt to add multipix ID %d to archaic pixel listcode %s",
				pixid, getListCode());
		}
		
		public Integer getAllowedPixel()
		{
			return _pixelId;
		}
	}	
	
	
	
	public static class UserRequest extends MultiPixRequest implements HasClientPixel
	{
		// TODO: this is really the adbListId
		private int _usrMultiId;
		
		UserRequest(String listcode)
		{
			super(StagingType.user);
			
			String[] toks = listcode.split("_");
			
			Util.massert(StagingType.valueOf(toks[0]) == StagingType.user,
				"Expected stagingtype user, found %s", toks[0]);
			
			_usrMultiId = Integer.valueOf(toks[1]);
		}
		
		public String getListCode()
		{
			return getListCode(_usrMultiId);	
		}
		
		public static String getListCode(int abdlistid)
		{
			return Util.varjoin("_", StagingType.user, abdlistid);	
		}
		
		public Integer getAllowedPixel()
		{
			return (_pixSet.isEmpty() ? null : _pixSet.first());	
		}
	}
	
	public static class SysMultiRequest extends MultiPixRequest
	{
		private String _vertCode;
		
		private CountryCode _ctryCode;
		
		SysMultiRequest(String listcode)
		{
			super(StagingType.sysmulti);
			
			String[] toks = listcode.split("_");
			
			Util.massert(StagingType.valueOf(toks[0]) == StagingType.sysmulti,
				"Expected stagingtype sysmulti, found %s", toks[0]);
			
			_vertCode = toks[1];
			
			// This is a little bit redundant with country code column
			_ctryCode = CountryCode.valueOf(toks[2]);
		}
		
		public String getListCode()
		{
			return Util.varjoin("_", StagingType.sysmulti, _vertCode, _ctryCode);	
		}
		
		public String getVertCode()
		{
			return _vertCode;	
		}
		
		// Ugh, back-override to make this eternal
		@Override
		public String getExpirationDate()
		{
			return "2100-01-01";
		}		
	}	
	

	public static class NegRequest extends ScanRequest
	{
		private CountryCode _ctryCode;
		
		private int _trancheId; // basically zero
		
		private NegRequest(CountryCode cc, int tid)
		{
			super(StagingType.negative);
			
			_ctryCode = cc;
			_trancheId = tid;
		}
		
		public String getListCode()
		{
			return Util.varjoin("_",
				StagingType.negative, _ctryCode.toString().toUpperCase(), 
				Util.padLeadingZeros(_trancheId, 3));
			
		}
		
		public CountryCode getCountryCode()
		{
			return _ctryCode;	
		}
				
		public String getEntryDate()
		{
			return "2000-01-01";	
		}
		
		public String getNickName()
		{
			return Util.sprintf("NEGATIVE_%s", _ctryCode.toString().toUpperCase());
		}
		
		public boolean hasGeoSkew()
		{
			return false;	
		}
		
		@Override
		public String getBasicInfo()
		{
			return Util.sprintf("Negative Request for %s", getCountryCode());
		}				
		
		@Override
		public boolean equals(Object nreq)
		{
			if(!(nreq instanceof NegRequest))
				{ return false; }
			
			return _ctryCode == ((NegRequest) nreq).getCountryCode();
		}
	}	
	
	private static void checkOkayS(String a, String b, boolean nullokay, String fieldname)
	{
		boolean isokay;
		
		if(a == null)
		{
			Util.massert(nullokay, "Nulls found but not legit for %s", fieldname);				
			isokay = (b == null);
		} else {
			isokay = a.equals(b);	
		}
		
		Util.massert(isokay, "Found A=%s but B=%s for %s", a, b, fieldname);
	}
	
	private static void checkOkayO(Object a, Object b, boolean nullokay, String fieldname)
	{
		checkOkayS(a == null ? null : ""+a, b==null ? null : ""+b, nullokay, fieldname);
	}
}
