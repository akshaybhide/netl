
package com.adnetik.analytics;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.DbUtil.*;
import com.adnetik.shared.Util.*;


// This code just downloads the data from HDFS and uploads it to the database
public class ThirdPartyDataUploader
{
	SortedMap<LineHolder, int[]> _lineMap = Util.treemap();
	
	private SimpleMail _logMail;
	private String _dayCode;
	private Integer _maxLine;
	
	private BufferedReader _dataRead;
	
	public static final String TABLE_NAME = "3p_icc";
	
	private Set<Part3Code> _partSet = Util.treeset();
	
	/*
	public static void main(String[] args) throws Exception
	{
		
		if(args.length < 1)
		{
			Util.pf("ThirdPartyDataUploader <daycode>\n");
			return;
		}
		
		String dc = args[0];
		dc = "yest".equals(dc) ? TimeUtil.getYesterdayCode() : dc;
		Util.massert(TimeUtil.checkDayCode(dc), "Invalid day code %s", dc);	
		
		Map<String, String> optargs = Util.getClArgMap(args);
		Integer maxline = optargs.containsKey("maxline") ? Integer.valueOf(optargs.get("maxline")) : Integer.MAX_VALUE;
		Util.pf("Using maxline=%d\n", maxline);
		
		ThirdPartyDataUploader tpdu = new ThirdPartyDataUploader(dc, maxline);
		tpdu.runProcess();

	}
	*/
	
	// TODO: why so many goddam constructors for this thing?
	public ThirdPartyDataUploader(String dc)
	{
		this(dc, Integer.MAX_VALUE);	
	}
	
	public ThirdPartyDataUploader(String dc, int maxline)
	{
		this(dc, maxline, Part3Code.values());
	}
	
	
	public ThirdPartyDataUploader(String dc, int maxline, Part3Code... partset)
	{
		this(dc, maxline, Arrays.asList(partset));
	}
	
	public ThirdPartyDataUploader(String dc, int maxline, Collection<Part3Code> partset)
	{
		_dayCode = dc;	
		_logMail = new SimpleMail(Util.sprintf("ThirdPartyDataUploader for %s, Vendors %s",
								_dayCode, Util.join(partset, ",")));
		_maxLine = maxline;

		for(Part3Code p3c : partset)
			{ _partSet.add(p3c); }		
	}
	
	public void removePartCode(Part3Code p3c)
	{
		Util.massert(_partSet.contains(p3c), "Part code %s already removed", p3c);
		_partSet.remove(p3c);
	}
	
	
	public void runProcess() throws IOException
	{
		deleteOld();		
		readData();
		writeInfData();
		doUpload();	
		_logMail.send2admin();
	}
	
	public static String getBasicUniqPath(String daycode)
	{
		return Util.sprintf("/thirdparty/uniqs/basic/%s/part-00000", daycode);
	}
	
	public String getInfDataPath()
	{
		return Util.sprintf("/home/burfoot/bluekai/infdata/inf_%s.tsv", _dayCode);
	}
	
	/// TODO: redo this using InfSpooler
	public List<String> getColList()
	{
		// Object[] rowdata = new Object[] { holdkey._campid, holdkey._segid, holdkey._3ptype, holdkey._linetype, uniq_total[0], uniq_total[1] };
		String[] clist = new String[] { "daycode", "campaign_id", "line_item_id", "seg_id", "3p_data_code", "linetypecode", "uniq_count", "total_count", "logtype" }; 	
		return Arrays.asList(clist);
	}
	
	List<Pair<LineHolder, int[]>> getHolderListFromLine(String oneline)
	{
		// <combkey>, part3, logtype, totalcount, seginfo
		
		List<Pair<LineHolder, int[]>> linelist = Util.vector();
		
		String[] toks = oneline.trim().split("\t");
		String[] wtp_camp_line_linetype = toks[0].split(Util.DUMB_SEP);
		int campid = Integer.valueOf(wtp_camp_line_linetype[1]);
		int lineid = Integer.valueOf(wtp_camp_line_linetype[2]);
		String linetype = wtp_camp_line_linetype[3];
				
		Part3Code pt3code = getPt3Code(toks[1]);	
		LogType logt = LogType.valueOf(toks[2]);
		int totalcount = Integer.valueOf(toks[3]);
		
		String[] seglist = toks[4].split(",");
		for(String oneseg : seglist)
		{
			int segid = Integer.valueOf(oneseg);
			LineHolder lhold = new LineHolder(campid, lineid, pt3code, segid, linetype, logt);

			int[] cts = new int[] { 1, totalcount }; 
			linelist.add(Pair.build(lhold, cts));
		}
		
		return linelist;
	}
	
	
	Part3Code getPt3Code(String fullcode)
	{
		if(fullcode.startsWith("bluekai"))
			{ return Part3Code.BK; }
		
		if(fullcode.startsWith("exelate"))
			{ return Part3Code.EX; }
		
		throw new RuntimeException("Unknown fullcode " + fullcode);
	}
	
	
	void initDataReader(String resfilepath) throws IOException
	{
		// FileSystem fsys = FileSystem.get(new Configuration());
		// BufferedReader _data = HadoopUtil.hdfsBufReader(fsys, getBasicUniqPath(_dayCode));		
		
		_dataRead = FileUtils.getReader(resfilepath);
	}
	
	void readData() throws IOException
	{
		Util.massert(_dataRead != null, "Must initialize data reader before calling");
		
		int lcount = 0;
		int maphit = 0;
			
		for(String oneline = _dataRead.readLine(); oneline != null; oneline = _dataRead.readLine())
		{
			// Util.pf("Line is %s\n", oneline);
			
			List<Pair<LineHolder, int[]>> holdlist = getHolderListFromLine(oneline);
			
			// Util.pf("Found %d lineholders\n", holdlist.size());
			
			for(Pair<LineHolder, int[]> holdpair : holdlist)
			{
				if(!_partSet.contains(holdpair._1._3ptype))
					{ continue; }
				
				// Util.pf("LHold code is %s\n", holdpair._1._compareCode);
				if(_lineMap.containsKey(holdpair._1))
				{
					maphit++;
					for(int i = 0; i < 2; i++)
					{
						_lineMap.get(holdpair._1)[i] += holdpair._2[i];
					}
					
				} else {
					_lineMap.put(holdpair._1, holdpair._2);
				}
			}
			
			lcount++;
			
			if((lcount % 10000) == 0)
			{
				_logMail.pf("Read %d total lines, %d map hits, total linemap size is %d\n", lcount, maphit, _lineMap.size());
			}
			
			if(lcount > _maxLine)
				{ break; }
		}
		
		_dataRead.close();
	}
	
	// TODO: redo using InfSpooler
	private void writeInfData() throws IOException
	{
		BufferedWriter infwrite = FileUtils.getWriter(getInfDataPath());
		for(LineHolder holdkey : _lineMap.keySet())
		{
			int[] uniq_total = _lineMap.get(holdkey);
			Object[] rowdata = new Object[] { _dayCode, holdkey._campid, holdkey._lineid, holdkey._segid, holdkey._3ptype, holdkey._linetype, uniq_total[0], uniq_total[1], holdkey._sformLogT};
			infwrite.write(Util.join(rowdata, "\t"));
			infwrite.write("\n");
		}
		infwrite.close();
	}
	
	
	private void deleteOld()
	{
		String sql = Util.sprintf("DELETE FROM %s WHERE daycode = '%s' AND 3p_data_code in ('%s')", 
			TABLE_NAME, _dayCode, Util.join(_partSet, "','"));
		
		int delrows = DbUtil.execSqlUpdate(sql, new Party3Db());
		_logMail.pf("Deleted %d rows of old data\n", delrows);
	}
	
	private void doUpload()
	{
		int uprows = DbUtil.loadFromFile(new File(getInfDataPath()), TABLE_NAME, getColList(), new Party3Db());
		_logMail.pf("Uploaded %d rows into table\n", uprows);
	}
	
	public static class LineHolder implements Comparable<LineHolder>
	{
		private static Map<LogType, String> _LOG_CODE_MAP = Util.treemap();
		
		// Codes to put in DB
		static {
			_LOG_CODE_MAP.put(LogType.imp, "imp");
			_LOG_CODE_MAP.put(LogType.click, "clk");
			_LOG_CODE_MAP.put(LogType.conversion, "cnv");
		}
		
		private int _campid;
		private int _lineid; 
		private int _segid;
		
		private Part3Code _3ptype;
		private String _linetype;
		
		private String  _sformLogT;
		
		private String _compareCode;
			
		public LineHolder(int cpid, int lineid, Part3Code pt3, int sid, String ltype, LogType logt)
		{
			_campid = cpid;
			_lineid = lineid;
			_segid = sid;
			
			_3ptype = pt3;
			_linetype = ltype;
			
			_sformLogT = _LOG_CODE_MAP.get(logt);
			
			_compareCode = Util.join(new Object[] { _campid, _lineid, _3ptype, _segid, _linetype, _sformLogT }, "__");
		}
		
		public int compareTo(LineHolder other)
		{
			return _compareCode.compareTo(other._compareCode);	
		}
	}
	
	public static class Party3Db implements DbUtil.ConnectionSource
	{
		public static Connection getConnection() throws SQLException
		{
			return DbUtil.getDbConnection("thorin-internal.digilant.com", "thirdparty");
		}
		
		public Connection createConnection() throws SQLException
		{
			return getConnection();	
		}			
	}
}
