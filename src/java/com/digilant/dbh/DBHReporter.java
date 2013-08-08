package com.digilant.dbh;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.rowset.CachedRowSet;


import com.adnetik.bm_etl.BleStructure;
import com.digilant.fastetl.FastUtil;
import com.digilant.fastetl.FastUtil.BlockCode;
import com.digilant.fastetl.FastUtil.MyLogType;
import com.digilant.fastetl.FileManager;
import com.adnetik.shared.BidLogEntry;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.Util.LogField;
import com.adnetik.shared.Util.LogType;
import com.adnetik.shared.Util.LogVersion;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.PathInfo;
import com.adnetik.shared.Util;
import com.digilant.mobile.DBConnection;
import com.digilant.mobile.MobileUtil;


public class DBHReporter {
	FileManager _fileMan;
	List<String> brokenfiles = new Vector<String>();
	protected static HashMap<String, HashMap<String, String>> _hmCatalog;
	protected List<String> _lQueries;
	protected static String _store_tbl_name;
	protected static String _info_tbl_name;
	protected static String _db_name;
	protected static String _machine_name;
	protected static Connection fast_conn;
	protected static Connection ad_conn;
	protected static Connection self_conn;
	public static HashMap<String,HashMap<String, String>> _hmDims;
	public static ArrayList<String> colnames;
	private String current_file;
	public static int brokenqueryno;
	private  int totalexno = 0;
	private String date;
	public static void main(String[] args) throws Exception
	{
		System.out.println("saloom'".replace("'", "3"));

		if(args.length < 3){
			System.out.println( "you need to pass date (as 2012-05-29) and the number of look back date \n");
			System.exit(1);
		}
		String configpath = args[0];
		String date = args[1];
		int lookback = Integer.parseInt(args[2]);
		DBHReporter.init("thorin-internal.digilant.com", "dbh","dbh_general","dbh_dimensions");
		DBHReporter pr = new DBHReporter(date, lookback, configpath, "thorin-internal.digilant.com", "pixel", "pixel_fast_general", "pixel_dimensions");
		
		
	}	
	public static void init(String machinename, String dbname, String store_tbl, String lookup_tbl) throws SQLException{
		brokenqueryno=0;
		_db_name = dbname;
		_info_tbl_name = lookup_tbl;
		_store_tbl_name = store_tbl;
		_machine_name = machinename;
		fast_conn = DBConnection.getConnection("thorin-internal.digilant.com", "fastetl");
		ad_conn = DBConnection.getConnection("thorin-internal.digilant.com", "adnetik");
		self_conn = DBConnection.getConnection(machinename, _db_name);
		_hmDims =  lookupColumns();
		_hmCatalog = loadCatalog();
		fast_conn.close();
		ad_conn.close();
		
		}

	public DBHReporter(String date, int lookback, String path, String machine, String db, String storetable, String infotable) throws Exception{
		_lQueries = new ArrayList<String>();
		_fileMan = new FileManager(path);
		this.date = date;
		loadCleanList();
		_fileMan.flushCleanList();
		startUp(date , lookback);
		System.out.println("writing remainder to Db\n");
		report();
		System.out.println("date: "+ date + "total exceptions:" +totalexno);
		MoveToCurrent();

	}
	
	private  static HashMap<String, HashMap<String, String>> lookupColumns(){
		
		try {
			colnames = new ArrayList<String>();
			String query = "select * from " + _info_tbl_name;
			CachedRowSet rs =  DBConnection.runQuery(_machine_name, _db_name, query);
			HashMap<String, HashMap<String, String>> result = new HashMap<String, HashMap<String, String>>(); 
			while(rs.next()){
				HashMap<String, String> onerow = new HashMap<String, String>();
				String colname = rs.getString(2).trim();
				onerow.put("logfield_name", rs.getString(3).trim());
				onerow.put("machine", rs.getString(4).trim());
				onerow.put("db", rs.getString(5).trim());
				onerow.put("query", rs.getString(6).trim());
				colnames.add(colname);
				if(colname.equals("_date"))
					colnames.add("id_time");
				result.put(colname, onerow);
				
			}
			return result;
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
		}			
		
	}

	protected static HashMap<String, HashMap<String, String>> loadCatalog() throws SQLException{
		HashMap<String, HashMap<String, String>> cat_map = new HashMap<String, HashMap<String, String>>();
		Set<String> cols = _hmDims.keySet();
		for(String col : cols){
			Connection active_conn;
			String query = _hmDims.get(col).get("query");
			String db = _hmDims.get(col).get("db");
			if(query.contains("NA"))
				continue;
			HashMap<String, String> table = new HashMap<String, String>();
			if(db.contains("adnetik"))
				active_conn = ad_conn;
			else if(db.contains("fastetl"))
				active_conn = fast_conn;
			else
				active_conn = self_conn;
			CachedRowSet rs =  DBConnection.runBatchQuery(active_conn, query);
			while(rs.next()){
				String val = rs.getString(1);
				if(val == null) continue;
				val = val.trim();
				String id = rs.getString(2).trim();
				table.put(val, id);
			}
			cat_map.put(col, table);
			
			
		}
		String query = "select Hour, Quartet, PK_Time from dim_Time";
		HashMap<String, String> table = new HashMap<String, String>();
		CachedRowSet rs =DBConnection.runBatchQuery(fast_conn, query);
		while(rs.next()){
			String val1 = rs.getString(1);
			String val2 = rs.getString(2);
			String val = val1+val2;
			String id = rs.getString(3).trim();
			table.put(val, id);
		}
		cat_map.put("id_time", table);

		return cat_map;
	}
	protected  static void writeDimNames( StringBuffer dimcols){
		int i = 0;
		for(String col : colnames){
			if(i > 0) 	
				dimcols.append(",");
			if(col.toLowerCase().contains("wtp")){
				dimcols.append("wtp1,wtp2, wtp3, wtp4, wtp5,wtp6,wtp7, wtp8, wtp9, wtp10 ");
			} 
			else
				dimcols.append(col);
			i++;
		}
		dimcols.append(",type");
	}	

	void report(){
		if(_lQueries.size() == 0 ) return;
		//System.out.println("rows no:" + _lQueries.size());
		StringBuffer constpart = new StringBuffer();
		StringBuffer cols = new StringBuffer();
		constpart.append("insert into ");
		constpart.append(_store_tbl_name);
		constpart.append("(");
		writeDimNames( cols);
		constpart.append(cols);
		constpart.append(") values ");
		StringBuffer bigquery = new StringBuffer();
		bigquery.append(constpart);
		int i = 1;
		int cumcnt = 0;
		for(String q : _lQueries){
			if(i > 1)
				bigquery.append(",");
			bigquery.append(q);
			i++;
			if(i%500==0){
				int wcnt = report(bigquery.toString());
				if(wcnt!=i-1)
					System.out.println("!!!!expected to insert " + (i -1) + " and inserted " + wcnt);
				cumcnt+=i-1;
				bigquery = new StringBuffer();
				bigquery.append(constpart);
				i = 1;
			}
			
		}
		int wcnt = 0;
		if(i > 1){
		 wcnt = report(bigquery.toString());
		 if(_lQueries.size() - cumcnt !=wcnt)	
			 System.out.println("expected to insert " + (_lQueries.size() - cumcnt) + " and inserted " + wcnt);
		}
		_lQueries.clear();
	}
	public static void wrapup(){
		try {
			self_conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	int report(String query){
		//System.out.println("writing to DB");
		ArrayList<Integer> cnt = new ArrayList<Integer>();
		cnt.add(0);
		try {
			if(!self_conn.isValid(2))
				self_conn = DBConnection.getConnection(_machine_name, _db_name);
			DBConnection.runBatchQuery(self_conn, query, cnt);
			return cnt.get(0);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println(brokenqueryno+" queries broken ");
			e.printStackTrace();
			ArrayList<String> q = new ArrayList<String>();
			//q.add(current_file);
			System.out.println(current_file +":exception happened");
			q.add(query.toString());
			FileUtils.createDirForPath(_fileMan.getBaseDir(false)+"/dbh"+brokenqueryno+".txt");
			try {
				FileUtils.writeFileLines(q, _fileMan.getBaseDir(false)+"/dbh"+brokenqueryno+".txt");
				brokenqueryno++;
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return 0;
	}
	void MoveToCurrent() throws Exception
	{
		{
			String jnkpath = _fileMan.getJunkPath();
			FileUtils.createDirForPath(jnkpath);
			FileUtils.createDirForPath(jnkpath);
			File jnk = new File(jnkpath);
			File cur = new File(_fileMan.getBaseDir(true));
			File stg = new File(_fileMan.getBaseDir(false));
			
			// TODO: delete junk directories
			
			// Should be very fast operation
			boolean cur_renamed = cur.renameTo(jnk);
			if(!cur_renamed){
				Util.pf("problem rename to junk path : %s \n", jnk.getAbsoluteFile());
				throw new Exception("problem rename to junk path");
			}
			boolean stg_renamed = stg.renameTo(cur);
			if(!stg_renamed){
				Util.pf("problem rename to current path\n");
				throw new Exception("problem rename to current path");
			}
			if(stg_renamed && cur_renamed)
				Util.pf("renamed directory %s --> %s\n", stg, cur);			
		}
	}	

	void startUp(String date, int lookback) 
	{
		MyLogType[] mlt = new MyLogType[]{MyLogType.click, MyLogType.imp, MyLogType.conversion, MyLogType.bid_all, MyLogType.no_bid};
//		ExcName[] en = ExcName.values();
		ExcName[] en = new ExcName[]{ExcName.dbh};
		Set<String> pathset = _fileMan.newFilesLookBack(date, lookback, mlt, en);
		List<String> pathlist = new Vector<String>(pathset);
		Collections.shuffle(pathlist);
		ArrayList<String> processed = new ArrayList<String>(); 
		System.out.printf("Found %d new paths\n", pathset.size());
		
		for(int i = 0;i < pathlist.size() ; i++)
		{
			String onepath  = pathlist.get(i);
			//Util.pf("processing %s\n", onepath);
			processFile(onepath, 1);
			report();
			try {
				_fileMan.flushCleanList(onepath);
				
				//Util.pf("flushing %s to cleanlist %d \n", onepath, i);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				Util.pf("!!problem flushing %s to cleanlist\n", onepath);
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				
				throw new RuntimeException("problem renaming folders, repititive data might be inserted to DB, try placing the last cleanlist.txt to current folder");
			}
			
			if((i % 10) == 0)
			{
				System.out.printf("Done with file %d/%d\n", i, pathlist.size());
			}
			//break;
			
			/*if(i > 20)
				{ 
					report();
					System.out.println("went over ~200 files\n");
					//break;
				}*/
		}
		Object[] copyofbrokenfiles = brokenfiles.toArray();
		for(int i = 0;i < copyofbrokenfiles.length ; i++){
			String onepath  = (String)copyofbrokenfiles[i];
			Util.pf("last attempt for file : %s , if doesn't work it is ignored till next 15 minutes\n", onepath);
			processFile(onepath, 501);
			
		}
		brokenfiles.clear();
	}
	void processFile(String filepath, int tryno)
	{
		
		// TODO: this is kind of ugly, shouldn't need to read the file twice.
		// Util.pf("Running for file %s\n", filepath);
		System.out.printf(".");
		
		try { 
			current_file = filepath;
			BufferedReader bread = FastUtil.getGzipReader(filepath);
			PathInfo pinfo = new PathInfo(filepath);
			add(bread, filepath, pinfo);
			
			bread.close();
			//_fileMan.reportFinished(filepath);
			
		} catch (IOException ioex) { 
			//Sleep 1 second and try 10 times
			System.out.println("Io Exception, did not flush to clean list");
			if (tryno > 10){
				brokenfiles.add(filepath);
				return;

			}
			else{
				try {
					Thread.sleep(1000);
					System.out.printf("Waiting 1 sec for file %s\n", filepath);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				processFile(filepath, ++tryno);
				
			}
		}		
		
	}
	void add(BufferedReader bread, String filepath, PathInfo pinfo) throws IOException{
		int lineno = 0;
		int good = 0;
		int bad = 0;
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			lineno++;
			if(processLogLine(oneline, filepath, pinfo, lineno))
				good++;//usefullineitem++;
			else{
				bad++;
				//System.out.println("exception at " + filepath + " line : " + lineno);
			}
		}
		System.out.println(filepath + " "+(lineno - good) +"/" + lineno+ " broken");

	}
	void loadCleanList() throws IOException
	{
		
		
		Set<String> savedata = _fileMan.getAggPathSet(true);
		System.out.printf("Found %d save data paths\n", savedata.size());
		_fileMan.reloadFromSaveDir();
		
	}
	
	boolean processLogLine(String oneline, String filename, PathInfo pinfo, int lineno)
	{
		Set<String> cols = _hmDims.keySet();
		StringBuffer constpart = new StringBuffer();
		StringBuffer valpart = new StringBuffer();
		BleStructure blestruct;
		BlockCode blockcode;
		boolean added = false;
		//constpart.append("replace into pixel_general (");
		valpart.append(" (");
		try {
//			blestruct = BleStructure.buildStructure(pinfo.pType, pinfo.pVers, oneline);
			BidLogEntry logentry = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
			int i = 0;
			for(String col:colnames){
				//if(col.equals("https")|| col.equals("segment_id")||col.equals("uuid")||col.equals("segment_type")) continue;
				if(col.toLowerCase().equals("id_time")) continue;
				//System.out.println(col);
				String val;
				if(col.toLowerCase().equals("wtp")){
					if(logentry.hasField("dbh_macro")){
						//int fid = logentry.getFieldId("dbh_macro");
						int fid = logentry.getFieldId(LogField.dbh_macro);
						int fcount = logentry.getFieldCount();
						val = (fid >= fcount ? "" : logentry.getField(LogField.dbh_macro));
					}
					else
						val = "";
				}
				else{
					if(pinfo.pType!=LogType.imp){
						if(col.toLowerCase().equals("imp_cost"))
							val = "";
						else if(col.toLowerCase().equals("won_bid"))
							val="0.0";
						else{
								if(col.toLowerCase().equals("entry_date")) val = pinfo.getDate();
								else	val=(logentry.hasField(LogField.valueOf(_hmDims.get(col.toLowerCase()).get("logfield_name").trim()))?logentry.getField(_hmDims.get(col.toLowerCase()).get("logfield_name").trim()):"");
							}
					}
					else{
						if(col.toLowerCase().equals("entry_date")) val = pinfo.getDate();
						else val=(logentry.hasField(_hmDims.get(col.toLowerCase()).get("logfield_name").trim())?logentry.getField(_hmDims.get(col.toLowerCase()).get("logfield_name").trim()):"");
						if(col.toLowerCase().equals("imp_cost")&& val!=null){
							if(!val.equals(""))
								val= (Double.parseDouble(val)/1000)+"";
						}
						else if(col.toLowerCase().equals("won_bid")&& val!=null){
							if(!val.equals(""))
								val= (Double.parseDouble(val)/1000)+"";
						}
					}
					
				}
				if(added){
					constpart.append(",");
					valpart.append(",");
					added = false;
					
				}
				if(col.toLowerCase().equals("_date")){
					 constpart.append(col);
					 blockcode = FastUtil.BlockCode.fromBlockKey(val);
					 valpart.append(MobileUtil.encloseInSingleQuotes(blockcode.getDaycode()));
					 constpart.append(", id_time");
					 valpart.append(",");
					 valpart.append(_hmCatalog.get("id_time").get(blockcode.getHour()+""+blockcode.getQuartet()));
						added = true;
					 continue;
				}
				if(col.toLowerCase().equals("entry_date")){
					 constpart.append(col);
					 val = pinfo.getDate();
		
				}
				if(col.toLowerCase().equals("wtp")){
					String[] wtpi;
					if(val==""){
						wtpi = new String[1];
						wtpi[0] = "";
					}
					else wtpi = val.split("\\|");
					for(int k = 1; k <=10; k++){
						boolean kwasthere = false;
						int l;
						if(k == 4){
							l=399;
						}
						else 
							l=299;
						for(String wtp:wtpi){
							if(wtp.equals("")) break;
							if(wtp.substring(0, wtp.indexOf("=")).contains(k+"")){
								 constpart.append("wtp"+k);
								 String v = wtp.substring(wtp.indexOf("=")+1).replace("'", "");
								 valpart.append(MobileUtil.encloseInSingleQuotes(v.substring(0, Math.min(l, v.length()))));
								 constpart.append(", ");
								 valpart.append(",");
								 kwasthere = true;
								 break;
							}
						}
						if(!kwasthere){
									 constpart.append("wtp"+k);
									 constpart.append(", ");
									 valpart.append("'-1',");
									
						}

					}
					continue;
				}
				if(col.toLowerCase().equals("imp_cost")){
					if(!pinfo.pType.toString().equals(LogType.imp.toString())){
						valpart.append("0.0,");
						continue;
					}
				}
/*				if(col.toLowerCase().equals("won_bid")){
					if(!pinfo.pType.toString().equals(LogType.imp.toString())){
						valpart.append("0.0,");
						continue;
					}
				}*/
				if(col.toLowerCase().equals("dbh_publisher_id")){
					if (val.length() > 10){
						System.out.println("too long publisher id");
						return false;
						
					}
				}
				String query = _hmDims.get(col.toLowerCase()).get("query").trim();
				if(!query.equals("NA")&& val.length()!=0){
					HashMap<String, String> table =_hmCatalog.get(col); 
					if(table == null){
						Util.pf("Table was null, is not supposed to be null\n");
						val = null;
						}
					else
						val = table.get(val);
					if(val==null)
						val = "NULL";
				}
				if(val.length()==0) val = "0";
				constpart.append(col);
				added = true;
				if(col.contains("id")||col.contains("cost")||col.contains("utw")|| col.contains("won_bid"))
					valpart.append(val);
				else{
					valpart.append(MobileUtil.encloseInSingleQuotes(val.replace("'", "")));
					}
			}
			constpart.append(")");
			//valpart.append(",");
			//valpart.append(MobileUtil.encloseInSingleQuotes(date));//this is for entry date
			valpart.append(",");
			valpart.append(MobileUtil.encloseInSingleQuotes(pinfo.pType.toString()));
			valpart.append(")");
			//System.out.println(valpart.toString());
			//System.out.println(constpart.toString() + valpart.toString());
			_lQueries.add(/*constpart.toString() + */valpart.toString());
			return true;
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			totalexno++;
			System.out.println("  line-> " + lineno);
			return false;
		}
	}	
	
	
	// TODO: probably a bad method, because it mixes Pixel IDs with Line Item IDs
	
	



}
