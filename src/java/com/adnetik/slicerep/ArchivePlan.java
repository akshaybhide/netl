
package com.adnetik.slicerep;

import java.util.*;
import java.io.*;
import java.sql.*;

//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.shared.BidLogEntry.*;

public class ArchivePlan
{
	private String _dayCode;
	
	private Map<AggType, List<Integer>> _campListMap = Util.treemap();
	
	public static int ARCHIVE_TIME_WINDOW = 210;
	
	SimpleMail _logMail;
	FileSystem _fSystem;
	
	
	public static void main(String[] args) throws IOException
	{
		String testdate = TimeUtil.nDaysBefore(TimeUtil.getTodayCode(), ARCHIVE_TIME_WINDOW);
			
		ArchivePlan aplan = new ArchivePlan(testdate);
		aplan.createBackupDir();
		
		for(AggType atype : AggType.values())
		{ 
			aplan.fullArchiveProcess(atype);
		}
			
		aplan._logMail.send2AdminList(AdminEmail.burfoot, AdminEmail.huimin);
	}
	
	private ArchivePlan(String dc) throws IOException
	{
		_dayCode = dc;
		TimeUtil.assertValidDayCode(dc);
		
		_fSystem = FileSystem.get(new Configuration());
		
		_logMail = new SimpleMail("ArchivePlanReport for " + _dayCode);
		
	}
	
	private void fullArchiveProcess(AggType atype) throws IOException
	{
		while(true)
		{
			Integer nextid = getNextCampId(atype);
			if(nextid == null)
				{ break; }
			
			createGimpTable(atype);
			
			insertInto(atype, nextid);
			
			dumpGimp2Hdfs(atype, nextid);
			
			deleteOld(atype, nextid);
		}
	}
	
	
	private Integer getNextCampId(AggType atype)
	{
		String sql = Util.sprintf("SELECT id_campaign FROM %s WHERE id_date = '%s' LIMIT 1",
			SliDatabase.getAggTableName(atype), _dayCode);
		
		List<Integer> onecampid = DbUtil.execSqlQuery(sql, new DatabaseBridge(DbTarget.internal));

		if(onecampid.isEmpty())
			{ return null;}
		
		_logMail.pf("Found next campaign ID = %d\n", onecampid.get(0));
		
		return onecampid.get(0);
	}

	private void createGimpTable(AggType atype)
	{
		{
			// Be careful!!!
			String dropsql = Util.sprintf("DROP TABLE IF EXISTS %s", getGimpTableName(atype));
			DbUtil.execSqlUpdate(dropsql, new DatabaseBridge(DbTarget.internal));
		}
		
		{
			String sql = Util.sprintf("CREATE TABLE %s LIKE %s", 
				getGimpTableName(atype), SliDatabase.getAggTableName(atype));
			
			_logMail.pf("SQL is %s\n", sql);
			
			DbUtil.execSqlUpdate(sql, new DatabaseBridge(DbTarget.internal));
		}
	}
	
	private String getGimpTableName(AggType atype)
	{
		return Util.sprintf("__gimp_%s_archive", atype);	
	}
	
	
	private void insertInto(AggType atype, int campid)
	{
		String sql = Util.sprintf("INSERT INTO %s SELECT * FROM %s %s",
					getGimpTableName(atype), SliDatabase.getAggTableName(atype),
					getWhereClause(atype, campid));
			
		int inrows = DbUtil.execSqlUpdate(sql, new DatabaseBridge(DbTarget.internal));
		
		_logMail.pf("Inserted %d rows for campid=%d\n", inrows, campid);
	}
	
	private void deleteOld(AggType atype, int campid)
	{
		String sql = Util.sprintf("DELETE FROM %s %s", 
			SliDatabase.getAggTableName(atype), getWhereClause(atype, campid));
		
		int delrows = DbUtil.execSqlUpdate(sql, new DatabaseBridge(DbTarget.internal));

		DbUtil.execSqlUpdate(sql, new DatabaseBridge(DbTarget.internal));
		
		_logMail.pf("Deleted %d rows for campid=%d\n", delrows, campid);
	}
	
	private String getWhereClause(AggType atype, int campid)
	{
		return Util.sprintf("WHERE ID_CAMPAIGN = %d AND id_date = '%s'",
					campid, _dayCode);
	}
	
	private String getLocalFilePath(AggType atype, int campid)
	{
		return Util.sprintf("/home/burfoot/slicerep/archive/back_%s_%s_%d.sql", atype, _dayCode, campid);	
	}
	
	private Path getHdfsPath(AggType atype, int campid)
	{
		return new Path(Util.sprintf("%s/arch_%s_%d.sql", getBackupDir(), atype, campid));	
	}
	
	private void dumpGimp2Hdfs(AggType atype, int campid) throws IOException
	{
		DbUtil.DbCred usercred = DbUtil.lookupCredential();
		
		String syscall = Util.sprintf("mysqldump -u %s -h thorin-internal.adnetik.com -p%s fastetl %s",
			usercred.getUserName(), usercred.getPassWord(),
			getGimpTableName(atype));
			
		List<String> innlist = Util.vector();
		List<String> outlist = Util.vector(); // This is going to be huge!!!
		List<String> errlist = Util.vector();
		
		Util.syscall(syscall, innlist, outlist, errlist);
		
		if(!errlist.isEmpty())
		{	
			for(String errline : errlist)
			{
				Util.pf(errline);	
				
			}
			throw new RuntimeException("Error in MYSQL DUMP call");
		}
		
		Path hdfspath = getHdfsPath(atype, campid);
		
		HadoopUtil.writeLinesToPath(outlist, _fSystem, hdfspath);
		
		_logMail.pf("Wrote %d recs to file %s, size is %d\n", 
			outlist.size(), hdfspath, _fSystem.getFileStatus(hdfspath).getLen());
	}
	
	private void createBackupDir() throws IOException
	{
		Path backdir = new Path(getBackupDir());
		_fSystem.mkdirs(backdir);
		
		_logMail.pf("Created HDFS backup dir %s\n", backdir);
	}
	
	private String getBackupDir()
	{
		return Util.sprintf("/bm_etl/backup/%s", _dayCode);	
	}

}
