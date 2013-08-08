
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

// Lots of path management logic for the three "thirdparty" packages 
// that all kind of work the same way
public class SegmentPathMan
{
	public enum Party3Type { 
		bluekai, exelate, digipixel, pix_ourdata, ipp_ourdata, iab_ourdata;
	
		public String getProcFilePath(String daycode)
		{
			return Util.sprintf("%s/preproc_%s.txt", getLocalDir(), daycode);
		}
		
		public String getSnapshotDir()
		{
			return Util.sprintf("/thirdparty/%s/snapshot", this);
		}
		
		public String getLocalDir()
		{
			return Util.sprintf("/local/fellowship/thirdparty/%s", this);
		}
	};
	
	private boolean _doGzip;
	
	private Party3Type _partyType;
	
	public SegmentPathMan(Party3Type pt, boolean dogzip)
	{
		_partyType = pt;	
		
		_doGzip = dogzip;
	}
	
	@Deprecated
	public SegmentPathMan(Party3Type pt)
	{
		this(pt, false);	
	}
	
	public String getSnapshotDir()
	{
		return _partyType.getSnapshotDir();
	}
	
	public String getLocalDir()
	{
		return _partyType.getLocalDir();
	}	
	
	public String getHdfsMasterPath(String daycode)
	{
		return getMasterPath(getSnapshotDir(), daycode);
	}
	
	public String getLocalMasterPath(String daycode)
	{
		return getMasterPath(getLocalDir(), daycode);
	}
	
	private String getMasterPath(String basepath, String daycode)
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode %s", daycode);
		return Util.sprintf("%s/MASTER_LIST_%s.txt%s", basepath, daycode, (_doGzip ? ".gz" : ""));
	}
	
	public BufferedReader getLocalMasterReader(String daycode) throws IOException
	{
		String locpath = getLocalMasterPath(daycode);
		
		return _doGzip ? FileUtils.getGzipReader(locpath) : FileUtils.getReader(locpath);
		
	}
	
	public BufferedReader getHdfsMasterReader(String daycode) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());		
		String hdfspath = getHdfsMasterPath(daycode);
		return (_doGzip ? HadoopUtil.getGzipReader(fsys, hdfspath) : 
							HadoopUtil.hdfsBufReader(fsys, hdfspath));
	}
	
	public String getGimpPath(String daycode)
	{
		return Util.sprintf("%s/__GIMP_MASTER_%s.txt%s", getLocalDir(), daycode, (_doGzip ? ".gz" : ""));
	}		
	
	
	public BufferedWriter getGimpWriter(String daycode) throws IOException
	{
		String gpath = getGimpPath(daycode);
		return _doGzip ? FileUtils.getGzipWriter(gpath) : 
					FileUtils.getWriter(gpath);
	}
	
	public void renameGimp2Master(String daycode)
	{
		File srcfile = new File(getGimpPath(daycode));
		File dstfile = new File(getLocalMasterPath(daycode));
				
		srcfile.renameTo(dstfile);		
	}
	
	public void uploadMaster(String daycode, SimpleMail logmail) throws IOException
	{
		FileSystem fsys  = FileSystem.get(new Configuration());
		
		Path srcpath = new Path("file://" + getLocalMasterPath(daycode));
		Path dstpath = new Path(getHdfsMasterPath(daycode));
		
		try {
			fsys.copyFromLocalFile(false, true, srcpath, dstpath);
			logmail.pf("Copy complete src: %s\n", srcpath);
			logmail.pf("              dst: %s\n", dstpath);
			
		} catch (IOException ioex) {
			
			logmail.pf("FAILED TO COPY TO HDFS %s", ioex.getMessage());
			logmail.addExceptionData(ioex);
		}		
	}
	
	public void deleteOldLocalMaster(String daycode, int numsave, SimpleMail logmail)
	{
		String oldday = TimeUtil.nDaysBefore(daycode, numsave);
		File oldfile = new File(getLocalMasterPath(oldday));
		
		if(oldfile.exists())
		{ 
			oldfile.delete(); 
			logmail.pf("Deleted old master path %s", oldfile.getAbsolutePath());
		}		
		
	}
}
