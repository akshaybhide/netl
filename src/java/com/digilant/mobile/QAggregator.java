package com.digilant.mobile;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import com.adnetik.bm_etl.BleStructure;
import com.adnetik.bm_etl.BmUtil.DblFact;
import com.adnetik.bm_etl.BmUtil.IntFact;
import com.adnetik.bm_etl.Metrics;
import com.digilant.fastetl.FileManager;
import com.adnetik.shared.BidLogEntry.BidLogFormatException;
import com.adnetik.shared.FileUtils;
import com.adnetik.shared.PathInfo;
import com.adnetik.shared.Util;

public class QAggregator {
	FileManager _fileman;
	// date.
	private static final int maxclickno = 15;
	HashMap<GeneralDimension, Metrics> _memMap = Util.hashmap();	
	private static  LinkedHashMap<String, String>  colnames_and_types;
	HashMap<String, Integer> _uuid_hm = Util.hashmap();	
	HashMap<String, Integer> _userip_hm = Util.hashmap();	
	public String machine;
	public String db;
	public String dest_table;
	public String info_table;
	public ArrayList<String> colnames;
	private String currentquery;
	private int brokenqueryno=0;
	protected boolean isclick = false;
	int MaxMemSize = 20000;
	
	public void setclick(){
		isclick = true;
	}
	public void unsetclick(){
		isclick=false;
	}
	public void cleanfraud(String date){
		Connection mconn;
		try {
			mconn = DBConnection.getConnection(machine, db);

			StringBuffer q = new StringBuffer();
			q.append("delete  from frauduuid ");
			q.append(" where  ");
			q.append("p_date=");
			q.append(MobileUtil.encloseInSingleQuotes(date));
			DBConnection.runBatchQuery(mconn, q.toString());

			q = new StringBuffer();
			q.append("delete  from frauduserip ");
			q.append(" where  ");
			q.append("p_date=");
			q.append(MobileUtil.encloseInSingleQuotes(date));
			DBConnection.runBatchQuery(mconn, q.toString());

			mconn.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public QAggregator(String machine, String db, String info_table, String dest_table, FileManager f){
		_fileman = f;
		this.machine = machine;
		this.db = db;
		this.dest_table = dest_table;
		this.info_table = info_table;
		try {
			colnames = DBConnection.lookupColumns(machine, db, dest_table);
			colnames_and_types = DBConnection.lookupColumnsAndTypes(machine, db, dest_table);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	boolean processLogLine(String oneline, PathInfo pinfo, String date) throws SQLException
	{
		GeneralDimension dagg;
		BleStructure blestruct;
		try {
			blestruct = BleStructure.buildStructure(pinfo.pType, pinfo.pVers, oneline);
			String d = blestruct.getLogEntry().getField("ua_device_type").toLowerCase();
			String dmodel = blestruct.getLogEntry().getField("ua_device_model").toLowerCase();
			String dmaker = blestruct.getLogEntry().getField("ua_device_maker").toLowerCase();
			String dos = blestruct.getLogEntry().getField("ua_os").toLowerCase();
			String dversion = blestruct.getLogEntry().getField("ua_os_version").toLowerCase();
			if(blestruct.getLogEntry().getField("ad_exchange").toLowerCase().equals("admeld"))
				return false;
			if(d==null&&dmodel==null&&dmaker==null)
				return false;
			if(d.equals("null")&&dmodel.equals("null")&&dmaker.equals("null"))
				return false;
			if(d.equals("")&&dmodel.equals("")&&dmaker.equals(""))
				return false;
				if(d.contains("pc"))
				return false;
			if(isclick){
				String uuid = blestruct.getLogEntry().getField("uuid");
				String userip = blestruct.getLogEntry().getField("user_ip");
				if(!uuid.isEmpty()){
					if(_uuid_hm.containsKey(uuid.toString())){
						int cno = _uuid_hm.get(uuid) + 1;
						if(cno > maxclickno){
							_uuid_hm.put(uuid, maxclickno);
							return false;
						}
					}
					_uuid_hm.put(uuid.toString(), 1);
					
				}else{
					if(_userip_hm.containsKey(userip)){
						int cno = _userip_hm.get(userip) + 1;
						if(cno > maxclickno){
						_userip_hm.put(userip, maxclickno);
						return false;
						}
					}
					else
						_userip_hm.put(userip, 1);
				}
				
			}
			/*int beginIndex = _fileman.config.getICCSrc().length() + 2;
			String filename = pinfo.getActualPath();
			Pattern datepat = Pattern.compile("[2][0-1][1-3][0-9]-[0-9][0-9]-[0-9][0-9]");
			Matcher matcher = datepat.matcher(filename.substring(beginIndex));
			String date = "";
			if(matcher.find()) {
		    	date = filename.substring(matcher.start()+beginIndex, matcher.end()+beginIndex);
		    }*/
			dagg = new GeneralDimension(blestruct.getLogEntry(), date);
			Metrics magg = blestruct.returnMetrics();
			magg.standardizeCostInfo(pinfo);

			if(_memMap.containsKey(dagg))
			{ 
				_memMap.get(dagg).add(magg); 
				//Util.pf(dagg.toString() +  " (samekey)\n" );
			
			}
		else
			{ 
				_memMap.put(dagg, magg);
				//Util.pf(dagg.toString() + " (newkey)\n" );
				//Util.pf("did not find the key : %d, adding \n", dagg.hashCode());

			}
			return true;
		} catch (BidLogFormatException e) {
			// TODO Auto-generated catch block
			Util.pf("BidLogFormatException happened , skipping the line\n");
		}
		return false;
	}	
	
	
	// TODO: probably a bad method, because it mixes Pixel IDs with Line Item IDs
	
	
	
	private void getDateInterval(String date, int lookback, StringBuffer start, StringBuffer end){
		
		String[] ymd = date.split("-"); 
		GregorianCalendar endcal = new GregorianCalendar(Integer.parseInt(ymd[0]),Integer.parseInt(ymd[1])-1,Integer.parseInt(ymd[2]) );
		GregorianCalendar startcal = (GregorianCalendar)endcal.clone();
		startcal.add(Calendar.DATE, -lookback);
		start.append(MobileUtil.getDateKeyString(startcal,""));
		end.append(MobileUtil.getDateKeyString(endcal,""));
	}
	
	
	void reloadFromDB(String date, int lookback, boolean onlylast15) throws IOException, SQLException
	{	
		
		String table = dest_table;
		Connection mconn = DBConnection.getConnection(machine, db);
		StringBuffer start = new StringBuffer();
		StringBuffer end = new StringBuffer();
		String today = MobileUtil.getDateKeyString(new GregorianCalendar(),"");
		int quartet = MobileUtil.getPrevQuartet();
		getDateInterval(date, lookback, start, end);
		String query = "";
		if(!onlylast15){
			mconn.close();
			return;
			//query = "select * from  mobile_hourly" +" where ID_DATE between " + start.toString() + " and " + end.toString() ;
		
		}else
			query = "select * from " + dest_table + " where _DATE="+ date + " and ID_TIME<=" +GeneralDimension._hmCatalog.get("ID_TIME").get(23+""+3) ;
//			query = "select * from " + dest_table +" where ID_DATE="+ today + " and ID_TIME=" + quartet;
		CachedRowSet rs = DBConnection.runBatchQuery(mconn, query.toString());
		query = "delete from " + dest_table +" where _DATE="+ date + " and ID_TIME<=" +GeneralDimension._hmCatalog.get("ID_TIME").get(23+"" + 3);
		DBConnection.runBatchQuery(mconn, query.toString());
		Set<String> dcols = GeneralDimension.getDimNames();
		Set<String> mcols  = getMetricsCols();
		load(rs, colnames,dcols, mcols);
	}		
	private Set<String> getMetricsCols(){
		HashSet<String> metcols = new HashSet<String>();
		IntFact a;
		for(IntFact i : IntFact.values()){
			metcols.add("NUM_"+i.toString().toUpperCase());
		}
		metcols.add("IMP_COST");
		metcols.add("IMP_BID_AMOUNT");
		metcols.add("IMP_DEAL_PRICE");
		return metcols;
	}
	private void load(CachedRowSet rs, ArrayList<String> allcols, Set<String> dcols, Set<String> mcols) throws SQLException{
		while(rs.next()){
			HashMap<String, String> _hmData = new HashMap<String, String>();
			
			Metrics m = new Metrics();
			int colcnt = 0;
			for(String col : allcols){
				colcnt++;
				String val = rs.getString(colcnt);
				if(dcols.contains(col)){
					if(val==null){
						_hmData.put( col, "NULL");
					}
					else if(val.toLowerCase().contains("null"))
						_hmData.put( col, "NULL");
					else
						_hmData.put(col, val);
				}else if(mcols.contains(col)){
					if(col.contains("NUM"))
						m.setField(IntFact.valueOf(col.toLowerCase().substring(col.indexOf("_") + 1)), Integer.parseInt(val));
					else
						m.setField(DblFact.valueOf(col.toLowerCase().substring(col.indexOf("_") + 1)), Double.parseDouble(val));

				}
			}
			_memMap.put(new GeneralDimension(_hmData), m);
		}	
		
	}
	public boolean isDateAlreadyProcessed(String date){
		try {
			String query = "select * from processed_dates where p_date='" + date+"'";
			CachedRowSet rs = DBConnection.runQuery(machine, db, query);
			return rs.next();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	public void loadfraud(String date){
		try {
			String query = "select * from frauduuid where p_date='" + date+"'";
			CachedRowSet rs = DBConnection.runQuery(machine, db, query);
			while(rs.next()){
				String uuid = rs.getString(2)+"";
				int cnt = rs.getInt(3);
				_uuid_hm.put(uuid, cnt);
			}
			query = "select * from frauduserip where p_date='" + date+"'";
			rs = DBConnection.runQuery(machine, db, query);
			while(rs.next()){
				String userip = rs.getString(2)+"";
				int cnt = rs.getInt(3);
				_userip_hm.put(userip, cnt);
			}
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	public void savefraud(String date){
		Connection mconn;
		if(_uuid_hm.isEmpty()) return;
		try {
			mconn = DBConnection.getConnection(machine, db);
			StringBuffer q = new StringBuffer();
			q.append("replace into frauduuid (p_date, uuid, clickno) values");
			int i = 0;
			for(String uuid : _uuid_hm.keySet()){
				if(i > 0)
					q.append(",");
				i++;
				q.append("(");
				q.append(MobileUtil.encloseInSingleQuotes(date));
				q.append(",");
				q.append(MobileUtil.encloseInSingleQuotes(uuid));
				q.append(",");
				q.append(_uuid_hm.get(uuid));
				q.append(")");
				
			}
			DBConnection.runBatchQuery(mconn, q.toString());

			_uuid_hm.clear();
			if(_userip_hm.isEmpty()) return;
			q = new StringBuffer();
			q.append("replace into frauduserip (p_date, ip, clickno) values");
			i = 0;
			for(String userip : _userip_hm.keySet()){
				if(i > 0)
					q.append(",");
				i++;
				q.append("(");
				q.append(MobileUtil.encloseInSingleQuotes(date));
				q.append(",");
				q.append(MobileUtil.encloseInSingleQuotes(userip));
				q.append(",");
				q.append(_userip_hm.get(userip));
				q.append(")");
				
			}
			DBConnection.runBatchQuery(mconn, q.toString());
			mconn.close();
			_userip_hm.clear();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
	public void report() throws SQLException{
		Connection mconn = DBConnection.getConnection(machine, db);
		ArrayList<Integer> cnt = new ArrayList<Integer>();
		cnt.add(0);
		int realcnt = _memMap.size();
		for(GeneralDimension dim:_memMap.keySet()){
			StringBuffer insquery = new StringBuffer();
			StringBuffer delquery = new StringBuffer();
			StringBuffer dimcols = new StringBuffer();
			StringBuffer dimvals = new StringBuffer();
			StringBuffer metcols = new StringBuffer();
			StringBuffer metvals = new StringBuffer();
			Map<String, String> colvalpairs = dim.getDim();
			writeDim(colvalpairs, dimcols, dimvals);
			//writeDelQuery(colvalpairs, delquery);
			//Util.pf("%s,%s\n", dimcols, dimvals);
			writeMetrics(_memMap.get(dim), metcols, metvals);
			insquery.append("insert into ");
			insquery.append(dest_table);
			insquery.append("(");
			insquery.append(dimcols);
			insquery.append(metcols);
			insquery.append(") values (");
			insquery.append(dimvals);
			insquery.append(metvals);
			insquery.append(")");
			//Util.pf("%s\n", delquery);
			ArrayList<Integer> tmp = new ArrayList<Integer>();
			//DBConnection.runBatchQuery(mconn, delquery.toString(), tmp);
			DBConnection.runBatchQuery(mconn, insquery.toString(), cnt);
		}
		Util.pf("Expected to insert %d records and insrted %d \n", realcnt, cnt.get(0));
		mconn.close();
	}

	public void batchreport() throws SQLException{
		if(_memMap.isEmpty()) return;
		Connection mconn;
			try {
				mconn = DBConnection.getConnection(machine , db);
				int total = 0;
				StringBuffer constdimcols = new StringBuffer();
				writeDimNames(GeneralDimension.getDimNames(), constdimcols);
				StringBuffer constmetcols = new StringBuffer();
				writeMetricsNames(constmetcols);
				
				StringBuffer insquery = new StringBuffer();
				insquery.append("insert into ");
				insquery.append(dest_table);
				insquery.append("(");
				insquery.append(constdimcols);
				insquery.append(constmetcols);
				
				insquery.append(") values ");
				int i = 1;
				ArrayList<Integer> cnt = new ArrayList<Integer>();
				cnt.add(0);
				
				for(GeneralDimension dim:_memMap.keySet()){
					if(i> 1){
						insquery.append(",");
					}
					insquery.append("(");
					StringBuffer dimvals = new StringBuffer();
					StringBuffer metvals = new StringBuffer();
					Map<String, String> colvalpairs = dim.getDim();
					writeDimBatch(colvalpairs, dimvals);
					insquery.append(dimvals);
					//Util.pf("%s,%s\n", dimcols, dimvals);
					writeMetrics(_memMap.get(dim), new StringBuffer(), metvals);
					insquery.append(metvals);

					insquery.append(")");
					i++;
					if(i%1000 == 0 ){
						currentquery = insquery.toString();
						//System.out.println(currentquery.toString());
						DBConnection.runBatchQuery(mconn, insquery.toString(), cnt);
						if(i-1 != cnt.get(0))
							Util.pf("!!!Expected to insert %d records but insrted %d \n", i - 1, cnt.get(0));
						total+=cnt.get(0);
						i = 1;
						cnt.clear();
						cnt.add(0);
						insquery = new StringBuffer();
						insquery.append("insert into ");
						insquery.append(dest_table);
						insquery.append("(");
						insquery.append(constdimcols);
						insquery.append(constmetcols);
						insquery.append(") values ");
					}

					//Util.pf("%s\n", delquery);
				}
				if(i > 1){
					cnt.clear();
					cnt.add(0);
					currentquery = insquery.toString();
					DBConnection.runBatchQuery(mconn, insquery.toString(), cnt);
					total+=cnt.get(0);
					if(_memMap.size()!=total)
						Util.pf("!!!Expected to insert %d records but insrted %d \n", _memMap.size(), total);
					Util.pf("Inserted %d records into DB \n", total);
				}
				_memMap.clear();
				//Util.pf("%s\n", insquery.toString());
				mconn.close();

			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				ArrayList<String> q = new ArrayList<String>();
				q.add(currentquery);
				FileUtils.createDirForPath(_fileman.getBaseDir(false)+ "/mobile_"+brokenqueryno+".txt");
				try {
					FileUtils.writeFileLines(q, _fileman.getBaseDir(false)+"/mobile_"+brokenqueryno+".txt");
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				brokenqueryno++;
				//throw new SQLException(e);
			}
	}

	
	protected void clearFraudData(){
		_uuid_hm.clear();
		_userip_hm.clear();

	}
	
	
	private void writeDim(Map<String, String> colvalpairs, StringBuffer dimcols, StringBuffer dimvals){
		for(String col : colnames){
			if((!colnames_and_types.get(col.toLowerCase()).contains("varchar")&&!col.toLowerCase().contains("date"))){
//			if(col.toLowerCase().contains("id_")||col.toLowerCase().contains("is_")||col.toLowerCase().contains("fb_")||col.toLowerCase().contains("deal")){
				if(colvalpairs.get(col).equals("NULL")) continue;//letting it get null value because these are numbers
				dimcols.append(col);
				dimcols.append(",");
				dimvals.append(colvalpairs.get(col));
				dimvals.append(",");
			}else{
				dimcols.append(col);
				dimcols.append(",");
				dimvals.append(MobileUtil.encloseInSingleQuotes(colvalpairs.get(col)));
				dimvals.append(",");
			}
		}
		
	}
	protected  void writeDimNames(Set<String> cols, StringBuffer dimcols){
		for(String col : colnames){
				if(cols.contains(col)){
					dimcols.append(col);
					dimcols.append(",");
				}
		}
	}	
	private void writeDimBatch(Map<String, String> colvalpairs, StringBuffer dimvals){
		for(String col : colnames){
			if(!colvalpairs.keySet().contains(col)) continue;
			if(col.toLowerCase().contains("id_")||col.toLowerCase().contains("is_")){
				if(colvalpairs.get(col).equals("NULL")){
					dimvals.append("NULL,");
					continue;
				}
				if(col.toLowerCase().contains("is_")&&Integer.parseInt(colvalpairs.get(col))>1){
					dimvals.append("NULL,");
					continue;
				}
				dimvals.append(colvalpairs.get(col));
				dimvals.append(",");
			}else{
				
				if(colvalpairs.get(col).toLowerCase().equals("null"))
					dimvals.append(colvalpairs.get(col));
				else
					dimvals.append(MobileUtil.encloseInSingleQuotes(colvalpairs.get(col)));
				dimvals.append(",");
			}
		}
		
	}

	
	
	private void writeMetrics(Metrics magg, StringBuffer metcols, StringBuffer metvals){
		IntFact a;
		for(IntFact i : IntFact.values()){
			
			metcols.append("NUM_");
			metcols.append(i.toString().toUpperCase());
			metcols.append(",");
			metvals.append(magg.getField(i));
			metvals.append(",");
		}
		metcols.append("IMP_COST,");
		metvals.append(magg.getField(DblFact.cost));
		metvals.append(",");
		metcols.append("IMP_BID_AMOUNT");
		metvals.append(magg.getField(DblFact.bid_amount));
		metvals.append(",");
		metcols.append("IMP_DEAL_PRICE,");
		metvals.append(magg.getField(DblFact.deal_price));
	}
	private void writeMetricsNames(StringBuffer metcols){
		IntFact a;
		for(IntFact i : IntFact.values()){
			
			metcols.append("NUM_");
			metcols.append(i.toString().toUpperCase());
			metcols.append(",");
		}
		metcols.append("IMP_COST, IMP_BID_AMOUNT, IMP_DEAL_PRICE");
	}

}
