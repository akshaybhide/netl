package com.digilant.mobile;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import javax.sql.rowset.CachedRowSet;

import com.digilant.fastetl.FastUtil;
import com.digilant.fastetl.FastUtil.BlockCode;
import com.adnetik.shared.BidLogEntry;
import com.adnetik.shared.LogEntry;
import com.adnetik.shared.Util;

public class GeneralDimension {
	private static String _tbl_name;
	private static String _db_name;
	private static String _machine_name;
	private static Connection fast_conn;
	private static Connection ad_conn;

	public static HashMap<String,HashMap<String, String>> _hmDims;
	protected static HashMap<String, HashMap<String, String>> _hmCatalog;
	private HashMap<String, String> _hmData;
	private BlockCode blockcode;
	public static void main(String[] args) throws SQLException{
		GeneralDimension.init("thorin-internal.digilant.com","mobile","mobile_dimensions");
		//GeneralDimension gd = new GeneralDimension(null);
		//System.out.println(gd._hmDims.toString());
	}
	public static void init(String machinename, String dbname, String lookup_tbl) throws SQLException{
	_db_name = dbname;
	_tbl_name = lookup_tbl;
	_machine_name = machinename;
	fast_conn = DBConnection.getConnection(machinename, "fastetl");
	ad_conn = DBConnection.getConnection("thorin-internal.digilant.com", "adnetik");

	_hmDims =  lookupColumns();
	_hmCatalog = loadCatalog();
	}
	public GeneralDimension(HashMap<String, String> hm){
		_hmData = hm;
	}
	public GeneralDimension(LogEntry le, String entry_date) throws SQLException{
		_hmData = new HashMap<String, String>();
		//fast_conn = fconn;
		//ad_conn = aconn;
		BidLogEntry ble = (BidLogEntry)le;
		String exchange = le.getField("ad_exchange");
		for(String dim : _hmDims.keySet()){
			//System.out.println("col name :" + dim + " in log :" + _hmDims.get(dim.trim()).get("logfield_name"));
			if(dim.toLowerCase().equals("id_time")||dim.toLowerCase().equals("entry_date"))
				continue;
			String val;
			if(dim.toLowerCase().equals("id_region")){
				String id_country = _hmCatalog.get("ID_COUNTRY").get(le.getField("country"));
				if(id_country==null)
					id_country="NULL";
				val = le.getField(_hmDims.get(dim.trim()).get("logfield_name").trim()) +"_" +id_country;
			}else{
					if(ble.hasField(_hmDims.get(dim.trim()).get("logfield_name"))){ 
						if(ble.getFieldId(_hmDims.get(dim.trim()).get("logfield_name")) >= ble.getFieldCount())
							val = "NULL";
						else
							val = le.getField(_hmDims.get(dim.trim()).get("logfield_name"));
					
					}else 
						val = "NULL";
				}
			if(dim.toLowerCase().equals("id_creative")){
				if(exchange.equals("adnexus"))
					val = "apn"+ val;
				else 
					val = "id-"+val;
			}
			if(dim.toLowerCase().equals("_date")){
				
				blockcode = FastUtil.BlockCode.fromBlockKey(val);
				_hmData.put("_DATE",blockcode.getDaycode());
				//String query = "select PK_Time from dim_Time where Hour= "+ blockcode.getHour() + " and Quartet="+ (blockcode.getQuartet());
				//Util.pf(query);
				//CachedRowSetImpl rs = DBConnection.runBatchQuery(fast_conn,query);
				//rs.next();
				
				_hmData.put("ID_TIME", _hmCatalog.get("ID_TIME").get(blockcode.getHour()+""+blockcode.getQuartet()));
				_hmData.put("ENTRY_DATE", entry_date);
				
				continue;
			}
			
			String query = _hmDims.get(dim).get("query").trim();
			if(!query.equals("NA")&& val.length()!=0){
//				query = query.replace("$", MobileUtil.encloseInSingleQuotes(val));
//				String machine = _hmDims.get(dim).get("machine");
//				String db = _hmDims.get(dim).get("db");
//				CachedRowSetImpl rs;
//				if(db.toLowerCase().equals("fastetl"))
//					 rs = DBConnection.runBatchQuery(fast_conn,query);
//				else
//					rs = DBConnection.runBatchQuery(ad_conn,query);
				//CachedRowSetImpl rs = DBConnection.runQuery(machine,db, query);
//				if(rs.next())
				//Util.pf(_hmCatalog.keySet().toString());
				HashMap<String, String> table =_hmCatalog.get(dim); 
				if(table == null){
					Util.pf("Table was null, is not supposed to be null\n");
					val = null;
					}
				else{
					String tmp = val;
					val = table.get(val);
					if(dim.toLowerCase().contains("id_creative") && val==null)
						val="-"+tmp.substring(3);
					}
				if(val==null)
					val="NULL";
			}
			if(val.length()==0) val = "NULL";
			_hmData.put(dim, val);
		}
	}
	public static void wrapup() throws SQLException{
		ad_conn.close();
		fast_conn.close();
	}
	private static HashMap<String, HashMap<String, String>> loadCatalog() throws SQLException{
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
			else
				active_conn = fast_conn;
			if(col.toLowerCase().contains("id_region")) continue;
			CachedRowSet rs = DBConnection.runBatchQuery(active_conn, query);
			if(col.equals("ID_CREATIVE")){
				while(rs.next()){
					String valid = rs.getInt(1)+"";
					String valapn = rs.getInt(2)+"";
					if(valid == null && valapn == null) continue;
					String creativeid = rs.getInt(3)+"";
					if(valid!=null)
						table.put("id-"+valid, creativeid);
					if(valapn!=null)
						table.put("apn"+valid, creativeid);
				}
				cat_map.put(col, table);
				continue;
			}
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
		CachedRowSet rs = DBConnection.runBatchQuery(fast_conn, query);
		while(rs.next()){
			String val1 = rs.getString(1);
			String val2 = rs.getString(2);
			String val = val1+val2;
			String id = rs.getString(3).trim();
			table.put(val, id);
		}
		cat_map.put("ID_TIME", table);
		
		query = _hmDims.get("ID_REGION").get("query");
		table = new HashMap<String, String>();
		rs = DBConnection.runBatchQuery(ad_conn, query);
		while(rs.next()){
			String val1 = rs.getString(1);
			String val2 = rs.getString(2);
			String val = val1+"_"+val2;
			String id = rs.getString(3).trim();
			table.put(val, id);
		}
		cat_map.put("ID_REGION", table);


		return cat_map;
	}
	private  static HashMap<String, HashMap<String, String>> lookupColumns(){
		
		try {
			String query = "select * from " + _tbl_name;
			CachedRowSet rs = DBConnection.runQuery(_machine_name, _db_name, query);
			HashMap<String, HashMap<String, String>> result = new HashMap<String, HashMap<String, String>>(); 
			while(rs.next()){
				HashMap<String, String> onerow = new HashMap<String, String>();
				String colname = rs.getString(2).trim();
				onerow.put("logfield_name", rs.getString(3).trim());
				onerow.put("machine", rs.getString(4).trim());
				onerow.put("db", rs.getString(5).trim());
				onerow.put("query", rs.getString(6).trim());
				if(!colname.toLowerCase().contains("num")&& !colname.toLowerCase().contains("imp"))
					result.put(colname, onerow);
			}
			return result;
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
		}			
		
	}
	public boolean equals(Object o){
		//return ((GeneralDimension)o).getDim().entrySet().equals(this._hmData.entrySet());
		HashMap<String, String> otherdim = ((GeneralDimension)o).getDim(); 
		for(String key :_hmData.keySet()){
			if(!otherdim.get(key).equals(_hmData.get(key)))
				return false;
		}
		return true;
	}
	public String toString(){
		StringBuffer sb = new StringBuffer();
		List<String> keylist = new Vector<String>(_hmDims.keySet());
		Collections.sort(keylist);
		
		
		for(String key : keylist){
			sb.append(key);
			sb.append("=");
			sb.append(_hmData.get(key));
			sb.append("\t");
		}
		return sb.toString();
	}
	public int hashCode(){
		//Util.pf(toString().hashCode()+"--------------------------\n");
		return toString().hashCode();
	}
	public HashMap<String, String> getDim(){
		return (HashMap<String, String>) _hmData;
	}
	public static Set<String> getDimNames(){
		return _hmDims.keySet();
	}

}
