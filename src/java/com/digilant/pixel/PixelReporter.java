package com.digilant.pixel;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.security.MessageDigest;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.sql.rowset.CachedRowSet;

import com.adnetik.bm_etl.BleStructure;
import com.digilant.fastetl.FastUtil;
import com.digilant.fastetl.FastUtil.BlockCode;
import com.digilant.fastetl.FastUtil.MyLogType;
import com.digilant.fastetl.FileManager;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.SimpleMail;
import com.adnetik.shared.Util;
import com.digilant.mobile.DBConnection;
import com.digilant.mobile.MobileUtil;
import com.digilant.ntzetl.NzUtil;


public class PixelReporter {
	FileManager _fileMan;
	List<String> brokenfiles = new Vector<String>();
	protected int brokenlines = 0;
	protected ArrayList<String> _arbrokenlines = new ArrayList<String>();
	protected static HashMap<String, HashMap<String, String>> _hmCatalog;
	protected List<String> _lQueries;
	protected static String _store_tbl_name;
	protected static String _info_tbl_name;
	protected static String _db_name;
	protected static String _db_type;
	protected static String _machine_name;
	protected static Connection fast_conn;
	protected static Connection ad_conn;
	protected static Connection self_conn;
	public static HashMap<String,HashMap<String, String>> _hmDims;
	public static ArrayList<String> colnames;
	private String current_file;
	public static int brokenqueryno;
	public static int nzfileno;
	private HashMap<String, Integer> _hmAgg = new HashMap<String, Integer>();
	private static boolean isnz = false;
	private static int _batch_query_no = 100000;
	private static String delim;
	int MaxMemSize = 200000;
	private String date;
	private int lookback;
	
	public static void main(String[] args) throws Exception
	{
		if(args.length < 3){
			System.out.println( "you need to pass date (as 2012-05-29) and the number of look back date \n");
			System.exit(1);
		}
		String configpath = args[0];
		String date = args[1];
		int lookback = Integer.parseInt(args[2]);
		PixelReporter.init("mysql","thorin-internal.digilant.com", "fastpixel","pixel_general","pixel_dimensions");
		PixelReporter pr = new PixelReporter(date, lookback, configpath, "thorin-internal.digilant.com", "pixel", "pixel_fast_general", "pixel_dimensions");
		
		
	}	
	public static void init(String db_type, String machinename, String dbname, String store_tbl, String lookup_tbl) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		
		
		nzfileno=0;
		brokenqueryno=0;
		_db_name = dbname;
		_info_tbl_name = lookup_tbl;
		_store_tbl_name = store_tbl;
		_machine_name = machinename;
		_db_type = db_type;
		if(_db_type.toLowerCase().contains("z"))
			isnz =true;
		if(!isnz){
			fast_conn = DBConnection.getConnection(machinename, "fastetl");
			ad_conn = DBConnection.getConnection("thorin-internal.digilant.com", "adnetik");
			self_conn = DBConnection.getConnection(machinename, _db_name);
			_batch_query_no = 1000;
			delim = ",";

			
		}else {
			fast_conn = DBConnection.getNZConnection(machinename, "fastetl");
			ad_conn = DBConnection.getNZConnection(machinename, "adnetik");
			self_conn = DBConnection.getNZConnection(machinename, _db_name);
			delim="\t";
			
		}
		_hmDims =  lookupColumns();
		_hmCatalog = loadCatalog();
		fast_conn.close();
		ad_conn.close();
		
		}

	public PixelReporter(String date, int lookback, String path, String machine, String db, String storetable, String infotable) throws Exception{
		//_lQueries = new ArrayList<String>();
		this.date = date;
		this.lookback = lookback;
		_fileMan = new FileManager(path);
		loadCleanList();
		startUp(date , lookback);
		System.out.println("number of broken lines:+"+ brokenlines+" \n");
		FileUtils.createDirForPath(_fileMan.getBaseDir(false)+"/pixelbrokenlines.txt");
		FileUtils.writeFileLines(_arbrokenlines, _fileMan.getBaseDir(false)+"/pixelbrokenlines.txt");

		System.out.println("writing to Db\n");
		report();
		System.out.println("done writing to Db\n");
		_fileMan.flushCleanList();
		MoveToCurrent();

	}
	
	private  static HashMap<String, HashMap<String, String>> lookupColumns(){
		
		try {
			colnames = new ArrayList<String>();
			colnames = DBConnection.lookupColumns(_db_type, self_conn, _store_tbl_name);
			colnames.remove("entry_date");
			colnames.remove("hits");
			colnames.remove("pixel_count");
			String query = "select column_name, logfield_name, machine, db, query from " + _info_tbl_name;
			//CachedRowSet rs = DBConnection.runQuery(_machine_name, _db_name, query);
			CachedRowSet rs = DBConnection.runBatchQuery(self_conn, query);
			HashMap<String, HashMap<String, String>> result = new HashMap<String, HashMap<String, String>>(); 
			while(rs.next()){
				HashMap<String, String> onerow = new HashMap<String, String>();
				String colname = rs.getString(1).trim();
				onerow.put("logfield_name", rs.getString(2).trim());
				onerow.put("machine", rs.getString(3).trim());
				onerow.put("db", rs.getString(4).trim());
				onerow.put("query", rs.getString(5).trim());
				//colnames.add(colname);
				//if(colname.toLowerCase().contains("_date")&&isnz)
					//colnames.add("hour");
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
			if(col.toLowerCase().equals("id_region")) continue;
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
		String query = "";
		HashMap<String, String> table = new HashMap<String, String>();
		CachedRowSet rs ;
		if(!isnz){
			query = "select Hour, Quartet, PK_Time from dim_Time";
			table = new HashMap<String, String>();
			rs =  DBConnection.runBatchQuery(fast_conn, query);
			while(rs.next()){
				String val1 = rs.getString(1);
				String val2 = rs.getString(2);
				String val = val1+val2;
				String id = rs.getString(3).trim();
				table.put(val, id);
			}
			cat_map.put("id_time", table);
		}
		if(isnz){
			query = "select code, country_id, id from dim_region";
			rs =  DBConnection.runBatchQuery(fast_conn, query);
		}
		else{
			query = "select code, country_id, id from region";
			rs =  DBConnection.runBatchQuery(ad_conn, query);
			}
		table = new HashMap<String, String>();
		while(rs.next()){
			String val1 = rs.getString(1);
			String val2 = rs.getString(2);
			String val = val1+"_"+val2;
			String id = rs.getString(3).trim();
			table.put(val, id);
		}
		cat_map.put("id_region", table);
		return cat_map;
	}
	protected  void writeDimNames( StringBuffer dimcols){
		int i = 0;
		for(String col : colnames){
			if(i > 0) 	
				dimcols.append(",");
			dimcols.append(col);
			i++;
		}
	}	

	void report(){
		List<String> qlist = Util.vector();
		if(_hmAgg.size() == 0) return;
		StringBuffer constpart = new StringBuffer();
		StringBuffer cols = new StringBuffer();
		constpart.append("insert into ");
		constpart.append(_store_tbl_name);
		constpart.append("(");
		writeDimNames( cols);
		constpart.append(cols);
		StringBuffer bigquery = new StringBuffer();
		if(!isnz){
			constpart.append(",entry_date, pixel_count) values ");
			bigquery.append(constpart);
			}
		//else
			//constpart.append(",entry_date, hits) values ");
		int i = 1;
		int cumcnt = 0;
		for(String q : _hmAgg.keySet()){
			if(i > 1)
				bigquery.append(delim);
			StringBuffer qb = new StringBuffer();
			int end = q.length();
			if(!isnz) end--;
			qb.append(q.substring(0, end));
			qb.append(delim);
			qb.append(_hmAgg.get(q));
			if(!isnz)
				qb.append(")");
			//System.out.println(q);
			//System.out.println(qb.toString());
			if(isnz)
				qlist.add(qb.toString());
			else
				bigquery.append(qb.toString());
			i++;
			if(i%_batch_query_no==0){
				//System.out.println(bigquery.toString());
					if(!isnz) report(bigquery.toString());					
					int wcnt = report(qlist);
					if((i-1)!=wcnt&&!isnz) 
						System.out.println("expected to insert " + (i -1) + " and inserted " + wcnt);
					
					cumcnt+=i-1;
					bigquery = new StringBuffer();
					qlist.clear();
					if(!isnz)
						bigquery.append(constpart);
					i = 1;
				
			}
			
		}
		int wcnt = 0;
		if(i > 1){
			if(!isnz) report(bigquery.toString());					
			else wcnt = report(qlist);
		 if((_hmAgg.size() - cumcnt)!=wcnt && !isnz)
			 System.out.println("expected to insert " + (_hmAgg.size() - cumcnt) + " and inserted " + wcnt);
		 System.out.println("Inserted almost " + _hmAgg.size() + " lines\n");
		}
		_hmAgg.clear();
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
			if(!isnz){
				if(!self_conn.isValid(2)){
					DBConnection.renewConnection(_db_type, _machine_name, _db_name);
					}
				DBConnection.runBatchQuery(self_conn, query, cnt);
			}
			return cnt.get(0);
		} catch (SQLException  e) {
			// TODO Auto-generated catch block
			System.out.println(brokenqueryno+" queries broken ");
			e.printStackTrace();
			ArrayList<String> q = new ArrayList<String>();
//			q.add(current_file);
			q.add(query.toString());
			FileUtils.createDirForPath(_fileMan.getBaseDir(false)+"/"+brokenqueryno+".txt");
			try {
				FileUtils.writeFileLines(q, _fileMan.getBaseDir(false)+"/"+brokenqueryno+".txt");
				brokenqueryno++;
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	int report(List<String> query){
		//System.out.println("writing to DB");
		ArrayList<Integer> cnt = new ArrayList<Integer>();
		cnt.add(0);
		try {
			if(!isnz){
				if(!self_conn.isValid(2)){
					DBConnection.renewConnection(_db_type, _machine_name, _db_name);
					}
				DBConnection.runBatchQuery(self_conn, query.get(0), cnt);
			}else{
				//String fname = _fileMan.getBaseDir(false)+"/"+ (nzfileno++)+".txt";
				String fname = _fileMan.getBaseDir(false)+"/tmp.txt";
				FileUtils.createDirForPath(fname);
				try {
					ArrayList<String> list = new ArrayList<String>();
					list.add("armita@digilant.com");
					FileUtils.writeFileLines(query, fname);
					SimpleMail _logMail = new SimpleMail();
					String email = "armita@digilant.com";
					List<String> tmp = Util.vector();
					tmp.add(email);
					_logMail.renewAdminList(list);
					_logMail.setPrint2Console(true);
					boolean r = NzUtil.LoadCommand(fname, "reporting","k4573F7B",_machine_name, _db_name,_store_tbl_name, _logMail);
					if(r)
						_logMail.send(email);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					System.out.println("didn't load to netezza");
					File f1 = new File(fname);
					String newfname = _fileMan.getBaseDir(false)+"/"+ (nzfileno++)+".txt";
					File f2 = new File(newfname);
					boolean success = f1.renameTo(f2);
					if(!success) System.out.println("the file is lost could not rename");
					e.printStackTrace();
				}
				
			}
			return cnt.get(0);
		} catch (SQLException  e) {
			// TODO Auto-generated catch block
			System.out.println(brokenqueryno+" queries broken ");
			e.printStackTrace();
			ArrayList<String> q = new ArrayList<String>();
//			q.add(current_file);
			q.add(query.toString());
			FileUtils.createDirForPath(_fileMan.getBaseDir(false)+"/"+brokenqueryno+".txt");
			try {
				FileUtils.writeFileLines(q, _fileMan.getBaseDir(false)+"/"+brokenqueryno+".txt");
				brokenqueryno++;
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
			if(!cur_renamed)
				Util.pf("problem rename to junk path : %s \n", jnk.getAbsoluteFile());
			boolean stg_renamed = stg.renameTo(cur);
			if(!stg_renamed)
				Util.pf("problem rename to current path\n");
			if(stg_renamed && cur_renamed)
				Util.pf("renamed directory %s --> %s\n", stg, cur);			
		}
	}	

	void startUp(String date, int lookback) 
	{
		MyLogType[] mlt = new MyLogType[]{MyLogType.pixel};
		Set<String> pathset = _fileMan.newFilesLookBack(date, lookback, mlt);
		List<String> pathlist = new Vector<String>(pathset);
		//Collections.shuffle(pathlist);
		ArrayList<String> processed = new ArrayList<String>(); 
		System.out.printf("Found %d new paths\n", pathset.size());
		
		for(int i = 0;i < pathlist.size() ; i++)
		{
			String onepath  = pathlist.get(i);
			//if(!onepath.toLowerCase().contains("pixel")) continue;
			//Util.pf("processing %s\n", onepath);
			processFile(onepath, 1);
			//report();
			if((i % 10) == 0)
			{
				System.out.printf("Done with file %d/%d\n", i, pathlist.size());
			}
			//break;
			
			if(i > 10 && AggregationWrapper.mode.equals("test"))
				{ 
					//report();
					System.out.println("went over ~10 files\n");
					break;
				}
			if(_hmAgg.size() > MaxMemSize){
				System.out.println("size getting too big, writing to Db");
				report();
				try {
					_fileMan.flushCleanList();
					MoveToCurrent();
					loadCleanList();
					startUp(date, lookback);
					break;

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
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
			//PathInfo pinfo = new PathInfo(filepath);
			add(bread, filepath);
			
			bread.close();
			_fileMan.reportFinished(filepath);
			
		} catch (IOException ioex) { 
			//Sleep 1 second and try 10 times
			if (tryno > 500){
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
	void add(BufferedReader bread, String filepath) throws IOException{
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			if(processLogLine(oneline, filepath))
				;//usefullineitem++;
		}

	}
	void loadCleanList() throws IOException
	{
		
		
		Set<String> savedata = _fileMan.getAggPathSet(true);
		System.out.printf("Found %d save data paths\n", savedata.size());
		_fileMan.reloadFromSaveDir();
		
	}
	
	boolean processLogLine(String oneline, String filename)
	{
		Set<String> cols = _hmDims.keySet();
		StringBuffer constpart = new StringBuffer();
		StringBuffer valpart = new StringBuffer();
		BleStructure blestruct;
		BlockCode blockcode;
		boolean added = false;
		//constpart.append("replace into pixel_general (");
		
		if(!isnz)
			valpart.append(" (");
		try {
			//blestruct = BleStructure.buildStructure(pinfo.pType, pinfo.pVers, oneline);
			GPixelLogEntry logentry = GPixelLogEntry.getOrNull(oneline, filename);
			int i = 0;
			String val;
			for(String col:colnames){
				//if(col.equals("https")|| col.equals("segment_id")||col.equals("uuid")||col.equals("segment_type")) continue;
				if(col.toLowerCase().equals("hour")) continue;
				if(col.toLowerCase().equals("id_region")){
					String id_country = _hmCatalog.get("id_country").get(logentry.getField("country"));
					if(id_country==null)
						id_country="NULL";
					val = logentry.getField(_hmDims.get(col).get("logfield_name").trim()) +"_" +id_country;
				}
				else
					val = logentry.getField(_hmDims.get(col).get("logfield_name").trim());
				if(added){
					constpart.append(delim);
					valpart.append(delim);
					added = false;
					
				}
				if(col.toLowerCase().contains("_date")){
					 constpart.append(col);
					 blockcode = FastUtil.BlockCode.fromBlockKey(val);
					 if(isnz){
						valpart.append(blockcode.getDaycode());
						valpart.append(delim);
					 	valpart.append(blockcode.getHour());
					 }else{
						 valpart.append(MobileUtil.encloseInSingleQuotes(blockcode.getDaycode()));
						 
					 }
						added = true;
					 continue;
				}
				String query = _hmDims.get(col).get("query").trim();
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
				if(val.length()==0) val = "NULL";
				constpart.append(col);
				if(col.contains("uuid")){
					if (val.length() > 40){ 
						System.out.println("skipping line because of uuid:" + val);
						return false;
						}
					else{
						if(isnz){
							List<BigInteger> list = NzUtil.wtptrans(val);
							if(!list.isEmpty())
								val = String.valueOf(list.get(0));
							else{
								
								//val="NULL";
								val = logentry.getField("useragent");
								//String uag = ble.getField(LogField.user_agent_hash);
								String uag = val;
								if (uag.length() >= 32) {
									MessageDigest m = MessageDigest.getInstance("MD5");
									m.update(uag.getBytes("UTF-8"),0,uag.length()>16? 16:uag.length());
									//System.out.println("m is : "+m.getDigestLength());
									byte[] mhash=null;
									mhash = m.digest();
									//for(int i = 0 ; i < mhash.length;i++)
										//System.out.println("mhash of :"+i+" is : " +mhash[i]);
									long hash = 0L;
									for (int j = 0;j<4;j++) {
										//System.out.println("hash at iteration "+j+" is :"+String.valueOf(hash));
										hash = hash << 8 | mhash[j] & 0x00000000000000FFL;
									    
									    //System.out.println(mhash[j]);
									}
									hash = hash + (-2L*(1L << 62));
								//onerow.add(String.valueOf(hash));
								val = String.valueOf(hash);
								//System.out.println("Inserting user agent" + val);
							    }
							    else {return false;}
							}
							}
					}
				}
				if(col.equals("segment_id")){
					if (val.toLowerCase().equals("null"))
					return false;
				}
				added = true;
                
				val = val.replaceAll("'", "`");
				val = val.replaceAll(",", " ");
				val = val.replaceAll(";", " ");
				val = val.replaceAll("\\(", " ");
				val = val.replaceAll("\\)", " ");
				val = val.replace("\\", "");
				if(col.startsWith("id") || col.toLowerCase().equals("uuid_l")) //uuid_l is in netezza.
					valpart.append(val);
				else{
					if(!isnz)
						valpart.append(MobileUtil.encloseInSingleQuotes(val));
					else
						valpart.append(val);
						
					}
			}
			//constpart.append(")");
			int beginIndex = _fileMan.config.getPixelSrc().length()+1;
			String date = filename.substring(beginIndex, beginIndex+10);
			valpart.append(delim);
			if(!isnz){
				valpart.append(MobileUtil.encloseInSingleQuotes(date));
				valpart.append(")");
			}else
				valpart.append(date);
				
			//System.out.println(valpart.toString());
			//System.out.println(constpart.toString() + valpart.toString());
			//_lQueries.add(/*constpart.toString() + */valpart.toString());
			if(_hmAgg.containsKey(valpart.toString())){
				int cnt = _hmAgg.get(valpart.toString());
				cnt++;
				//System.out.println(cnt);
				_hmAgg.put(valpart.toString(), cnt);
			}
			else
				_hmAgg.put(valpart.toString(), 1);
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("logline broken");
			e.printStackTrace();
			brokenlines++;
			_arbrokenlines.add(oneline);
		}
		return false;
	}	
	
	
	// TODO: probably a bad method, because it mixes Pixel IDs with Line Item IDs
	
	




}
