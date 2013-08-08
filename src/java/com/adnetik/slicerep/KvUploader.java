package com.adnetik.slicerep;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place
import com.adnetik.slicerep.SliUtil.*; // Put all the enums in the same place

/**
 * Transform output of Hadoop job into a format compatible with SQL Infile operation
 */
public class KvUploader implements UploaderInterface
{	
	public static final int LINES_PER_BATCH = 1000000;	
	
	
	// Okay, there are lots of weird things going on with this INF path business.
	// The whole issue revolves around the difficulty of deleting files through SCP, which seems to be hard.
	// Also, we want every JVM to get its own remote inf path. This is so the backfill
	// doesn't interfere with the continuous mode system. 
	// private static final String STANDARD_INF_PATH = gimmeInfPath();
	private static Map<String, Map<AggType, String>> _JVM_UNIQ_INF_PATH = Util.treemap();
	
	AggType _aType;
	
	String _entryDate;
	
	Boolean _targetStaging = null;
	
	Set<DimCode> _dimSet;
	Set<IntFact> _intSet;
	Set<DblFact> _dblSet;
	
	Set<Integer> _campIdSet = Util.treeset();
	
	SortedSet<PseudoDimCode> _pseudoDimSet = Util.treeset();
	
	// Distinct quarter codes for a given upload
	Set<Integer> _quarterSet = Util.treeset();
	
	// This is for generating rand99 data
	Random _randGen = new Random();
	
	DbTarget _dbTarg;
	
	//BufferedWriter curBatch;
	// int _curBatchCount = 0;
	// String _infPath;
	
	UploaderBox _upBox;
	
	int totalIn = 0;
	int totalUp = 0;
	
	private int _linesPerBatch = LINES_PER_BATCH;
	
	SimpleMail _logMail;
	
	public static void main(String[] args) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());	
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);	
		SimpleMail mylogmail = new SimpleMail("KvUploader Report for " + daycode);
			
		String partpatt = Util.sprintf("/bm_etl/output/%s/intern_ad_general/part-*", daycode);
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, partpatt);
						
		mylogmail.pf("Found %d paths for pattern %s\n", pathlist.size(), partpatt);
		
		ArgMap optmap = Util.getClArgMap(args);
		boolean deleteold = optmap.getBoolean("deleteold", true);
		
		// This is kind of long-winded
		DbTarget mytarget = DbTarget.valueOf(optmap.getString("dbtarget", DbTarget.internal.toString()));
		
		if(pathlist.isEmpty())
			{ return; }

		// Delete old data
		if(deleteold) 
		{ 
			BatchDeleter bdel = new BatchDeleter(daycode, mylogmail);
			bdel.setDbTarget(mytarget);
			bdel.doDelete(false);
		}	
				
		Map<AggType, KvUploader> aggMap = Util.treemap();
		
		for(AggType oneagg : AggType.values())
		{
			// Target-Staging = false
			KvUploader kvup = new KvUploader(oneagg, daycode, false, mytarget);
			// kvup._linesPerBatch = 100000;
			kvup.setLogMail(mylogmail);
			aggMap.put(oneagg, kvup);
		}		
		
		for(Path onepath : pathlist)
		{
			BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, onepath);
			
			int lcount = 0;
			
			for(String oneline =  bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				String[] key_val = oneline.split("\t");
				
				boolean found = false;
				for(AggType at : AggType.values())
				{
					if(!found && key_val[0].indexOf(at.toString()) > -1)
					{
						aggMap.get(at).send(key_val[0], key_val[1]);	
						found = true;
					}
				}
				
				Util.massert(found, "Could not find aggtype code in KV line %s", oneline);
				lcount++;
			}
			
			mylogmail.pf("done with path %s\n", onepath);
			bread.close();
		}
		
		for(KvUploader kvup : aggMap.values())
			{ kvup.finish(); }		
		
		mylogmail.send(getEtlAlertList());
	}	
		
	public int getTotalIn()
	{
		return totalIn;	
	}
	
	public int getTotalUp()
	{
		return totalUp;	
	}
	
	private static List<String> getEtlAlertList()
	{
		List<String> alertlist = Util.vector();
		alertlist.add("raj.joshi@digilant.com");
		alertlist.add("aubrey.jaffer@digilant.com");
		alertlist.add("sekhar.krothapalli@digilant.com");
		alertlist.add("daniel.burfoot@digilant.com");
		return alertlist;
	}
	
	// TODO: does it ever make sense to use the fully spelled out version of the constructor...?
	public KvUploader(AggType at, String dc, boolean staging) throws IOException
	{	
		this(at, dc, staging, DbTarget.internal);
	}
	
	public KvUploader(AggType at, String dc, boolean staging, DbTarget dbtarg) throws IOException
	{
		Util.massert(dc != null, "Null entry date");
		
		_entryDate = dc;
		_aType = at;
		
		_dimSet = DatabaseBridge.getDimSet(at, dbtarg);
		_intSet = DatabaseBridge.getIntFactSet(at, dbtarg);
		_dblSet = DatabaseBridge.getDblFactSet(at, dbtarg);
		
		_dbTarg = dbtarg;
		
		// _upBox = new DirectUploader();
		_upBox = new ScpUploader(dbtarg);
		
		// Okay, we're going to have this object even in non-daily mode,
		// but in non-daily mode we just don't send it.
		_logMail = new SimpleMail("KvUploaderReport EntryDate=" + _entryDate);
		
		_targetStaging = staging;
		
		setPseudoDims();		
	}
	
	/*
	public void setDbTarget(DbTarget dbt) throws IOException
	{
		_upBox = new ScpUploader(dbt);
	}
	*/
	
	/*
	public void setHostName(String haddr) throws IOException
	{
		_upBox = (haddr == null ? new ScpUploader() : new ScpUploader(haddr));
	}
	*/
	
	void setLogMail(SimpleMail smail)
	{
		_logMail = smail;	
	}
	
	private static synchronized String getJvmUniqInfPath(String hostname, AggType atype)
	{
		if(!_JVM_UNIQ_INF_PATH.containsKey(hostname))
		{
			Map<AggType, String> submap = Util.treemap();
			
			for(AggType at : AggType.values())
				{ submap.put(at, SliUtil.getNewTempInfPath(at)); }
			
			_JVM_UNIQ_INF_PATH.put(hostname, submap);
		}

		return _JVM_UNIQ_INF_PATH.get(hostname).get(atype);
	}
	
	// These are things like ENTRY_DATE, RAND99, HAS_CC that don't count exactly as either
	// real dimensions or facts 	
	private void setPseudoDims()
	{
		Util.massert(_targetStaging != null, "Must set target-staging parameter ahead of time");
		
		Set<String> colnameset = DbUtil.getColNameSet(getTargTableName(), new DatabaseBridge(DbTarget.internal));
		
		_pseudoDimSet.clear();
		
		for(PseudoDimCode pdc : PseudoDimCode.values())
		{
			// This check is just for the sake of paranoia, can take it out if it causes problems
			Util.massert(!colnameset.contains(pdc.toString().toLowerCase()), 
				"Column naming mismatch: should be uppercase");
			
			if(colnameset.contains(pdc.toString().toUpperCase()))
				{ _pseudoDimSet.add(pdc); }
		}
		
		// _logMail.pf("Reset PseudoDimSet for %s, new set is %s\n", _aType, _pseudoDimSet);
	}
	
	// Call when you have a sorted, preaggregated output of key/value pairs
	void send(String daggqs, String maggqs) throws IOException
	{
		send(BmUtil.getParseMap(daggqs), maggqs);
	}
	
	void send(Map<String, String> pmap, String maggval) throws IOException
	{
		Metrics magg = Metrics.fromQueryStr(maggval);
		send(pmap, magg);
	}
	
	void send(Map<String, String> pmap, Metrics onemet) throws IOException
	{
		String atype = pmap.get("aggtype");
		Util.massert(atype != null, "Must set aggtype in dim_agg query string");
		send(AggType.valueOf(atype), pmap, onemet);
	}
	
	public void send(AggType atype, Map<String, String> pmap, Metrics onemet) throws IOException
	{
		// Now we output the 
		StringBuilder sb = new StringBuilder();
		{			
			{
				Integer datecheck = Integer.valueOf(pmap.get(DimCode.date.toString()));
				Util.massert(20120000 <= datecheck && datecheck <= 20150000, "Found out of range date %d", datecheck);
			}			
			
			for(DimCode dimc : _dimSet)
			{
				String dimkey = dimc.toString();
				Util.massert(pmap.containsKey(dimkey), "Dimension key not found: %s, map is %s", dimkey, pmap);

				// TODO: check that these are all legit integers...?
				String relval = pmap.get(dimkey);
				
				if(dimc == DimCode.domain)
				{
					if(relval.length() > 200)
						{ relval = relval.substring(0, 200); }
					
					// Strip out backslashes, which are usually only included because of data corruption
					if(relval.indexOf("\\") > -1)
						{ relval = relval.replaceAll("\\\\", ""); }
				}
				
				sb.append(relval).append("\t");
				
				try {
					if(dimc == DimCode.campaign)
					{
						int cid = Integer.valueOf(relval);	
						_campIdSet.add(cid);
						
						/*
						if(_aType == AggType.ad_general)
							{ KvCsvLog.getGenCsvLog(_entryDate).recordEntry(cid, onemet); }
						
						if(_aType == AggType.ad_domain)
							{ KvCsvLog.getDomCsvLog(_entryDate).recordEntry(cid, onemet); }
							
						*/
					}
					
					if(dimc == DimCode.quarter)
					{
						int qid = Integer.valueOf(relval);
						_quarterSet.add(qid);
					}
					
				} catch (NumberFormatException nfex) {
					Util.pf("Bad campaign or quarter value: %s\n", relval);
					return;
				}
			}
		}
		
		{
			for(IntFact ikey : _intSet)
			{						
				sb.append(onemet.getField(ikey));
				sb.append("\t");
			}
			
			for(DblFact dkey : _dblSet)
			{
				sb.append(onemet.getField(dkey));
				sb.append("\t");
			}
		}
		

		if(_pseudoDimSet.contains(PseudoDimCode.has_cc))
		{
			int cc = onemet.getField(IntFact.clicks) + onemet.getField(IntFact.conversions);
			int has_cc = (cc > 0 ? 1 : 0);
			sb.append(has_cc);
			sb.append("\t");			
		}
		
		if(_pseudoDimSet.contains(PseudoDimCode.rand99))
		{
			int r99 = _randGen.nextInt(100);
			sb.append(r99);
			sb.append("\t");
		}
		
		// Tack on the entry date to the last item in the list
		{
			Util.massert(_pseudoDimSet.contains(PseudoDimCode.entry_date), 
				"All table should have pseudo-dim entry_date");
			sb.append(_entryDate);
		}
				
		_upBox.addRecord(sb.toString());
		totalIn++;
				
		if((totalIn % _linesPerBatch) == 0)
		{
			// Util.pf("Finished with batch, flushing...\n");
			_upBox.flush();
		}
	}
	

	
	List<String> getColumnList()
	{
		List<String> collist = Util.vector();
		
		for(DimCode dcode : _dimSet)
		{ 
			// If usebase == true, we use the "unassigned_int_1" strings
			// instead of the ID_FBPAGETYPE strings
			collist.add(_targetStaging ? dcode.getSmartColName() : dcode.getBaseTableColName());
		}
		
		for(IntFact ifact : _intSet)
			{ collist.add("NUM_" + ifact); }
		
		for(DblFact dfact : _dblSet)
			{ collist.add("IMP_" + dfact); }
		
		// Careful!! The order of this add process must mirror these extra fields are added in send(...)
		if(_pseudoDimSet.contains(PseudoDimCode.has_cc))
		{
			collist.add(PseudoDimCode.has_cc.toString().toUpperCase());
		}
		
		if(_pseudoDimSet.contains(PseudoDimCode.rand99))
		{
			collist.add(PseudoDimCode.rand99.toString().toUpperCase());
		}
		
		collist.add(PseudoDimCode.entry_date.toString().toUpperCase());		
		return collist;
	}
	
	public void finish() throws IOException
	{
		_upBox.finish();	
		
		// KvCsvLog.getGenCsvLog(_entryDate).writeCsv(SliUtil.getKvCsvLogPath(AggType.ad_general, _entryDate));
		// KvCsvLog.getDomCsvLog(_entryDate).writeCsv(SliUtil.getKvCsvLogPath(AggType.ad_domain, _entryDate));
	}

	private String getTargTableName()
	{
		String basename = SliDatabase.getAggTableName(_aType);		
		return basename + (_targetStaging ? "_stage" : "");
	}
	
	interface UploaderBox
	{
		public abstract void flush() throws IOException;
		public abstract void finish() throws IOException;
		public abstract void addRecord(String recline) throws IOException;
	}
	
	// "Dumb" uploader that just uses direct Mysql calls
	// TODO: should be able to replace this with DbUtil.InfSpooler
	// Okay, the two UploaderBox classes can be combined into one.
	// The only difference is that one uses an SCP call to upload the file to the DB machine,
	// and then loads it locally; while the other loads it remotely.
	private class DirectUploader implements UploaderBox
	{
		BufferedWriter _curBatch;
		int _curBatchCount = 0;
		String _infPath;		
		
		protected String _hostAddr;
		
		public DirectUploader(DbTarget dbtarg) throws IOException
		{
			String haddr = DatabaseBridge.getHostName(dbtarg);
			
			_infPath = getJvmUniqInfPath(haddr, _aType);
			_curBatch = FileUtils.getWriter(_infPath);	
			
			_hostAddr = haddr;
		}
		
		// Override to close AND upload data file
		protected void closeBatch() throws IOException
		{
			_curBatch.close();
		}
		
		public void flush() throws IOException
		{
			// Close writer, upload, reopen
			closeBatch();
			
			_inf2Database();
			
			_curBatch = FileUtils.getWriter(_infPath);
			_curBatchCount = 0;			
		}		
		
		public void finish() throws IOException
		{
			closeBatch();
			
			_inf2Database();
			
			(new File(_infPath)).delete();
		}		
		
		public void addRecord(String recline) throws IOException
		{
			_curBatch.write(recline);
			_curBatch.write("\n");
			_curBatchCount++;	
		}			
				
		// This means the file is local to the ETL machine, ie the client
		public boolean isLocal()
		{ 
			return true;	
		}
		
		protected int _inf2Database()
		{
			List<String> collist = getColumnList();
			
			for(int i = 0; i < 10; i++)
			{
				try { 
					double startup = Util.curtime();
					
					Connection conn = DatabaseBridge.getDbConnection(_hostAddr, "fastetl");
					// Connection conn = SliDatabase.getConnection();
					File inffile = new File(_infPath);
					
					String loadsql = DbUtil.loadFromFileSql(inffile, getTargTableName(), collist, isLocal());
					
					int uprows = DbUtil.execSqlUpdate(loadsql, conn);
					conn.close();
					
					// Util.pf("Load File SQL is %s\n", loadsql);
					
					_logMail.pf("Uploaded %d rows out of %d in-lines for aggtype %s to haddr=%s, total-in=%d, took %.03f\n", 
						uprows, _curBatchCount, _aType, _hostAddr, totalIn, (Util.curtime()-startup)/1000);
					
					totalUp += uprows; 
					return uprows;
					
				} catch (SQLException sqlex) {
					
					_logMail.pf("Hit SQL exception %s, retrying \n", sqlex.getMessage());
					
					// Is this really going to do anything...?
					Util.sleepEat(5000);
				}
			}
			
			throw new RuntimeException("SQL upload failed after 10 retries");
		}			
	}
	
	// This is a slight modification of DirectUploader that transfers the INF file using SCP 
	// before doing the upload
	private class ScpUploader extends DirectUploader implements UploaderBox
	{		
		private static final String PRIVATE_KEY_PATH = "/home/burfoot/.ssh/priv_key_342";
		
		public ScpUploader(DbTarget dbtarg) throws IOException
		{
			super(dbtarg);
			
			Util.massert((new File(PRIVATE_KEY_PATH)).exists(), 
				"Private key file %s not found, required for use by ScpUploader", PRIVATE_KEY_PATH);
		}		
		
		@Override 
		public boolean isLocal()
		{
			// Not local = not on client machine, but remote machine
			return false;
		}
		
		@Override
		protected void closeBatch() throws IOException
		{
			super.closeBatch();	
			
			_uploadInfFile();
		}
		
		private void _uploadInfFile()
		{
			String scpcall = Util.sprintf("scp -i %s %s burfoot@%s:%s", 
				PRIVATE_KEY_PATH, _infPath, _hostAddr, _infPath);
			
			// Util.pf("Here is the scpcall \n%s\n", scpcall);
			
			try {
				double startup = Util.curtime();
				Util.Syscall usys = new Util.Syscall(scpcall);
				usys.exec();
				
				_logMail.pf("SCP'ed inffile to DB machine %s, took %.03f\n", _hostAddr, (Util.curtime()-startup)/1000);
				
			} catch (IOException ioex) {
				throw new RuntimeException(ioex);	
			}			
		}		
	}	
	
	public static class KvCsvLog
	{
		private static TreeMap<String, KvCsvLog> _GEN_CACHE = Util.treemap();
		private static TreeMap<String, KvCsvLog> _DOM_CACHE = Util.treemap();


		TreeMap<Integer, Integer[]> _dataMap = Util.treemap();
			
		public static synchronized KvCsvLog getGenCsvLog(String entrydate)
		{
			return getCsvLog(_GEN_CACHE, entrydate);
		}
		
		public static synchronized KvCsvLog getDomCsvLog(String entrydate)
		{
			return getCsvLog(_DOM_CACHE, entrydate);
		}		
		
		// This shouldn't really need to be synchronized, but...
		private static synchronized KvCsvLog getCsvLog(TreeMap<String, KvCsvLog> REL_CACHE, String entrydate)
		{
			if(!REL_CACHE.containsKey(entrydate))
			{
				REL_CACHE.put(entrydate, new KvCsvLog());	
				
				while(REL_CACHE.size() > 6)
					{ REL_CACHE.pollFirstEntry(); }
			}
			
			return REL_CACHE.get(entrydate);
		}
		
		public void recordEntry(int campid, Metrics mets)
		{
			if(!_dataMap.containsKey(campid))
			{
				Integer[] recarr = new Integer[IntFact.values().length];
				for(int i = 0; i < recarr.length; i++)
					{  recarr[i] = 0; }

				_dataMap.put(campid, recarr);
			}
						
			for(int i = 0; i < IntFact.values().length; i++)
				{  _dataMap.get(campid)[i] += mets.getField(IntFact.values()[i]); }
		}
		
		public void writeCsv(String writepath)
		{
			List<String> csvlines = Util.vector();
			
			for(int campid : _dataMap.keySet())
			{
				String oneline = campid + "," + Util.join(_dataMap.get(campid), ",");
				csvlines.add(oneline);
			}
			
			FileUtils.writeFileLinesE(csvlines, writepath);
		}
		
		public boolean equals(KvCsvLog other)
		{
			if(!_dataMap.keySet().equals(other._dataMap.keySet()))
			{
				Util.pf("Campaign ID maps are not equal \n%s\n%s\n", 
					_dataMap.keySet(), other._dataMap.keySet());
				return false;
			}
			
			for(int campid : _dataMap.keySet())
			{
				for(int i = 0; i < IntFact.values().length; i++)
				{
					int a = _dataMap.get(campid)[i];
					int b = other._dataMap.get(campid)[i];
					
					if(a != b)
					{
						Util.pf("Error for campid=%d, intfact=%s\nthis=%d,that=%d", campid, 
							IntFact.values()[i], a, b);
						return false;
					}
				}
			}
			
			Util.pf("CSV logs are equal\n");
			return true;
		}
	}
}
