
package com.adnetik.data_management;

import java.io.*;
import java.util.*;
import java.sql.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.DbUtil.*;
import com.adnetik.shared.BidLogEntry.*;

public class ParamPixelMan
{
	
	public static final String PARAM_PIX_BASE_TABLE = "pxprm_base";
	public static final String PARAM_PIX_KEYV_TABLE = "pxprm_keyv";
	
	public static final String PARAM_PIX_BASE_INF = "/home/burfoot/dataman/pixparam/PIX_BASE_INF.txt";
	public static final String PARAM_PIX_KEYV_INF = "/home/burfoot/dataman/pixparam/PIX_KEYV_INF.txt";

	// Keep track of simple errors, avoid crushing the logmail
	private int _basicErrCount = 0;
	
	public enum BaseField { nfsdateDate, pixfire_idInt, pix_idInt, wtp_idStr }
	
	public enum KeyvField { nfsdateDate, pixfire_idInt, param_keyStr, param_valStr }
	
	private TreeMap<Integer, PixPack> _pixelPackMap = Util.treemap();
	
	private String _dayCode;
	
	private InfSpooler _baseInfSpool = new InfSpooler(BaseField.values(), PARAM_PIX_BASE_INF, new ParamPixDb(), PARAM_PIX_BASE_TABLE);
	private InfSpooler _keyvInfSpool = new InfSpooler(KeyvField.values(), PARAM_PIX_KEYV_INF, new ParamPixDb(), PARAM_PIX_KEYV_TABLE);
	
	private int _subFireId = 0;
	
	private int _fileId = 0;
	
	private SimpleMail _logMail;
			
	public static void main(String[] args) throws Exception
	{
		String daycode = args[0];
		daycode = "yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode;
		
		ParamPixelMan ppm = new ParamPixelMan(daycode);
		ppm.fullProcess();
	}
	
	public ParamPixelMan(String dc)
	{
		_dayCode = dc;
		TimeUtil.assertValidDayCode(_dayCode);
			
		_logMail = new SimpleMail("ParamPixelManReport for " + _dayCode);
		
		_baseInfSpool.setBatchSize(100000);
		_keyvInfSpool.setBatchSize(100000);
		
		_baseInfSpool.setLogMail(_logMail);
		_keyvInfSpool.setLogMail(_logMail);
	}
	
	void fullProcess()
	{
		try {
			deletePrevData();
			processDayData();
		} catch (Exception ioex) {
			
			_logMail.pf("Hit exception on fileid=%d", _fileId);
			_logMail.addExceptionData(ioex);
		}
		
		_logMail.pf("Basic Error Count: %d\n", _basicErrCount);
		_logMail.send2admin();
	}
	
	void processDayData() throws IOException
	{
		List<String> pixlist = Util.getNfsPixelLogPaths(_dayCode);
		Collections.shuffle(pixlist);
		double startup = Util.curtime();
		
		for( ; _fileId < pixlist.size(); _fileId++)
		{
			String onepath = pixlist.get(_fileId);
			
			processFile(onepath);
			
			// Here, we send only if the param-map has reached a certain size
			if(_pixelPackMap.size() > 10000)
				{ sendData(); }
			
			if((_fileId % 5) == 0)
			{
				_logMail.pf("Finished with path %d/%d, avg is %.03f secs/file, est complete %s\n",
					_fileId, pixlist.size(), (Util.curtime()-startup)/((_fileId+1)*1000), 
					TimeUtil.getEstCompletedTime(_fileId, pixlist.size(), startup));
			}
		}
		
		// Definitely send the rest of the data
		sendData();
		
		// SendData guarantees that all the Base data will be uploaded, but not the KV data.
		_keyvInfSpool.finish();
		
		Util.massert(_pixelPackMap.size() == 0, "Did not clear the pix pack map");
	}

	private void deletePrevData()
	{
		deleteDayData(_dayCode, _logMail);
		
		// Clean up old data from long ago
		
		String prevday = TimeUtil.nDaysBefore(_dayCode, 90);
		deleteDayData(prevday, _logMail);
	}
	
	private static void deleteDayData(String daycode, SimpleMail logmail)
	{
		// NB you have to delete from the KEYVAL table before deleting from PIX_BASE
		for(String onetable : new String[] { PARAM_PIX_KEYV_TABLE, PARAM_PIX_BASE_TABLE  })
		{
			String sql = Util.sprintf("DELETE FROM %s WHERE nfsdate = '%s'",
				onetable, daycode);
			
			logmail.pf("Del sql is %s\n", sql);
			
			int numdel = DbUtil.execSqlUpdate(sql, new ParamPixDb());
			
			logmail.pf("Deleted %d old rows for daycode=%s\n", numdel, daycode);
		}
	}
	
	private static Map<String, String> getParamMap(PixelLogEntry ple)
	{
		String requri = ple.getField(LogField.request_uri);
		int questind = requri.indexOf("?");
		
		if(questind == -1)
			{  return null; }

		String substr = requri.substring(questind+1);

		// TODO: can write a generic key-value splitter, it's used in a lot of places
		Map<String, String> mymap = Util.treemap();
		for(String onekv : substr.split("&"))
		{
			String[] kv = onekv.trim().split("=");
			if(kv.length != 2 || kv[0].length() == 0 || kv[1].length() == 1)
				{ continue; }
			
			mymap.put(kv[0], kv[1]);
		}
		
		// Util.pf("requri is %s, substr is %s, mymap is %s\n", requri, substr, mymap);
		
		return mymap;
	}
	
	private void logBasicError(String pfstr, Object... vargs)
	{
		_basicErrCount++;
		
		// Avoid crushing the AdminMail
		if(_basicErrCount < 30)
			{ _logMail.pf(pfstr, vargs); }
	}
	
	private void processFile(String onepixpath) throws IOException
	{
		BufferedReader bread = FileUtils.getGzipReader(onepixpath);
		
		for(String logline = bread.readLine(); logline != null; logline = bread.readLine())
		{
			PixelLogEntry ple = PixelLogEntry.getOrNull(logline);
			if(ple == null)
				{ continue; }
			
			String wtpstr = ple.getField(LogField.wtp_user_id);
			WtpId wid = WtpId.getOrNull(wtpstr);
			if(wid == null)
				{ continue; }

			int pixid = ple.getIntField(LogField.pixel_id);
			
			Map<String, String> paramdata = getParamMap(ple);
			
			if(paramdata == null || !paramdata.containsKey("id"))
			{ 
				logBasicError("Bad parammap %s, pixid=%d, fileid %d\n", paramdata, pixid, _fileId);
				continue;
			}
						
			// Don't need to put this in the param_key table
			paramdata.remove("id");
			
			// No param data, so quit: no point in putting data in table that doesn't have param info
			if(paramdata.isEmpty())
				{ continue; }
			
			// subpixfireid is just the ID within the particular day
			PixPack ppack = new PixPack(_subFireId, pixid, wid);
			ppack.addParamData(paramdata);
			
			Util.putNoDup(_pixelPackMap, _subFireId, ppack);
			
			_subFireId++;			
		} 
		
		bread.close();
		
	}
	
	private void sendData() throws IOException
	{
		// Okay, the main gotcha here is that we need to upload
		// the base data before the keyv data, because of the foreign key
		// constraint.
		// So we need to force the InfSpooler to upload the base data, 
		// even if if doesn't have that much info in the batch
		
		for(PixPack ppack : _pixelPackMap.values())
			{ ppack.sendBase2Inf(_baseInfSpool, _dayCode); }

		_baseInfSpool.upload2db();
		
		for(PixPack ppack : _pixelPackMap.values())
			{ ppack.sendKeyv2Inf(_keyvInfSpool, _dayCode); }
			
		// Just call _keyvInfoSpool.finish() at the end of the entire process
		// _keyvInfSpool.upload2db();
		
		// This is important to free up memory
		_pixelPackMap.clear();
	}
	
	public static class QueryRunner
	{
		int _pixId;
		String _dayCode;
		
		public QueryRunner(int pixid)
		{
			this(pixid, "2000-01-01");	
		}
		
		public QueryRunner(int pixid, String dc)
		{
			_pixId = pixid;
			_dayCode = dc;
			
			TimeUtil.assertValidDayCode(dc);
		}
		
		private String getBasicQuery()
		{
			return Util.sprintf("SELECT distinct(wtp_id) FROM v__combined WHERE pix_id = %d AND nfsdate > '%s'", _pixId, _dayCode);
			
		}
		
		public SortedSet<Pair<Long, String>> getValueSet(String key)
		{
			String sql = Util.sprintf("SELECT count(*), param_val FROM v__combined WHERE pix_id = %s AND nfsdate > '%s' AND param_key = '%s' group by param_val", 
				_pixId, _dayCode, key);
			
			List<Pair<Long, String>> vlist = DbUtil.execSqlQueryPair(sql, new ParamPixDb());
			
			TreeSet<Pair<Long, String>> valset = Util.treeset();
			
			for(Pair<Long, String> onepair : vlist)
				{ valset.add(onepair); }
			
			return valset;
		}
		
		public List<String> getKeySet()
		{
			String sql = Util.sprintf("SELECT distinct(param_key) FROM v__combined WHERE pix_id = %s AND nfsdate > '%s'", 
				_pixId, _dayCode);
			
			return DbUtil.execSqlQuery(sql, new ParamPixDb());
		}
		
		public TreeSet<WtpId> get4KeyVal(String key, String val)
		{
			TreeSet<WtpId> idset = Util.treeset();
			
			String sql = Util.sprintf("%s AND param_key = '%s' AND param_val = '%s'",
				getBasicQuery(), key, val);
			
			// Util.pf("SQL query is %s\n", sql);
			
			List<String> idlist = DbUtil.execSqlQuery(sql, new ParamPixDb());
			
			for(String oneid : idlist)
			{	
				WtpId wid = WtpId.getOrNull(oneid);
				if(wid != null)
					{ idset.add(wid); }
			}
			
			return idset;
		}
	}
	
	
	
	private static class PixPack
	{
		// The unique ID of this FIRING
		int _pixFireId;
		
		// The ID of the actual pixel
		int _pixId; 
		
		WtpId _wtpId;
				
		Map<String, String> _parMap = Util.treemap();
		
		
		PixPack(int pxfid, int pixid, WtpId wid)
		{
			_pixFireId = pxfid;
			_pixId = pixid;
			_wtpId = wid;
		}
		
		String getBaseInsertSql(String daycode) 
		{
			return Util.sprintf("INSERT INTO %s (pixfire_id, pix_id, wtp_id, daycode) VALUES (%d, %d, '%s', '%s');",
							PARAM_PIX_BASE_TABLE, _pixFireId, _pixId, _wtpId.toString(), daycode);
			
		}
		
		void sendBase2Inf(InfSpooler infspool, String daycode) throws IOException
		{
			infspool.setInt(BaseField.pixfire_idInt, _pixFireId);
			infspool.setInt(BaseField.pix_idInt, _pixId);
			infspool.setStr(BaseField.wtp_idStr, _wtpId.toString());
			
			infspool.setDate(BaseField.nfsdateDate, daycode);
			infspool.flushRow();
		}
		
		void sendKeyv2Inf(InfSpooler infspool, String daycode) throws IOException
		{
			for(Map.Entry<String, String> pent : _parMap.entrySet())
			{
				infspool.setDate(KeyvField.nfsdateDate, daycode);
				infspool.setInt(KeyvField.pixfire_idInt, _pixFireId);
				infspool.setStr(KeyvField.param_keyStr, pent.getKey());
				infspool.setStr(KeyvField.param_valStr, pent.getValue());	
				infspool.flushRow();
			}
		}		
		
		void addParamData(Map<String, String> kv)
		{
			_parMap.putAll(kv);
		}
	}
	
	public static class ParamPixDb implements DbUtil.ConnectionSource
	{
		public static Connection getConnection() throws SQLException
		{
			return DbUtil.getDbConnection("thorin.adnetik.com", "pixparam");
		}
	
		public Connection createConnection() throws SQLException
		{
			return getConnection();	
		}

	}
}
