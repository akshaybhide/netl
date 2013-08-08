
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

import com.adnetik.data_management.HdfsCleanup;
import com.adnetik.data_management.HdfsCleanup.*;


import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


public class CleanUpAidx
{	
	FileSystem _fSystem; 
	
	private enum AidxDirSaveInfo implements HdfsCleanup.DirSavePolicy
	{
		ADACLASS ("/userindex/adaclass", 90), // Not that big
		DBSLICE ("/userindex/dbslice", 21), // no point in saving longer than sortscrub
		EVALUATION ("/userindex/evaluation", 90), // This gets entered into Reporting DB
		FEATMAN ("/userindex/featman", 90), // This gets entered into Reporting DB
		FINALSTEP ("/userindex/finalstep", 90), // these are generated once per week, 
		LIFTREPORT ("/userindex/liftreport", 90), // these are generated once per week,
		NEGPOOLS ("/userindex/negpools", 100), // need these for a long time 
		PRECOMP ("/userindex/precomp", 85), // Keep these much longer than normal, there is only one per week
		SORTSCRUB ("/userindex/sortscrub", 21), // This is the biggest directory
		
		STAGING ("/userindex/staging", 45), // Keep these much longer than normal, there is only one per week
		USERSCORES ("/userindex/userscores", 35);


		public final String hdfspath;
		public final int savedays;
		
		AidxDirSaveInfo(String hpath, int sdays)
		{
			hdfspath = hpath;
			savedays = sdays;
		}
		
		public String getRootDir()
		{
			return hdfspath;	
		}
		
		public int getSaveDays()
		{
			return savedays;	
		}		
	}
	
	
	public static void main(String[] args) throws IOException
	{
		HdfsCleanup hclean = new HdfsCleanup();
		
		hclean.setLogMail(new DayLogMail(new CleanUpAidx(), TimeUtil.getTodayCode()));
		
		for(DirSavePolicy dpol : AidxDirSaveInfo.values())
		{
			hclean.cleanDirectory(dpol);	
		}
		
		hclean.getLogMail().send2AdminList(AdminEmail.burfoot);
	}
	
}
