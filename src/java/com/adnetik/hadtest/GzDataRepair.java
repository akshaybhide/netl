
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;

public class GzDataRepair
{			
	private String _hdfsPattern;
	
	private List<Path> _pathList = Util.vector();
	
	private FileSystem _fSystem = FileSystem.get(new Configuration());
	
	private Random _jRand = new Random();
		
	public static void main(String[] args) throws IOException
	{
		Util.pf("Hello, GZ data repair\n");
		
		GzDataRepair gdr = new GzDataRepair("/userindex/sortscrub/2012-09-18/*.txt.gz");
		
		gdr.doRepairs();
	}
	
	public GzDataRepair(String hpattern) throws IOException
	{
		_hdfsPattern = hpattern;
			
		_pathList = HadoopUtil.getGlobPathList(_fSystem, _hdfsPattern);
		
		Util.pf("Found %d paths to repair\n", _pathList.size());
		
	}
	
	private void doRepairs()
	{
		try {
			for(Path onepath : _pathList)	
			{
				SingleUpdate sup = new SingleUpdate(onepath);
				
				sup.doRepair();
				
				// break;
			}
		} catch (IOException ioex) {
			
			throw new RuntimeException(ioex);	
		}
	}
	

	
	private class SingleUpdate
	{
		Path _origPath;
		
		Path _reprPath;
		
		boolean _needRepair = true;
		
		// Max absolute difference in bytes between original and repaired version.
		public static final long MAX_FILE_DIFF = 5000L;
		
		private SingleUpdate(Path hpath)
		{
			_origPath = hpath;
			_reprPath = new Path(generateReprPath());
		}
		

		private long getOrigHdfsSize() throws IOException
		{
			return _fSystem.getFileStatus(_origPath).getLen();
		}
		
		private long getRepairedSize() throws IOException
		{
			return _fSystem.getFileStatus(_reprPath).getLen();			
		}
		
		private boolean checkOkay() throws IOException
		{
			long a = getRepairedSize();
			long b = getOrigHdfsSize();
			long d = a - b;
			d = (d < 0 ? -d : d);
			
			return d < MAX_FILE_DIFF;
		}
		
		private void doRepair() throws IOException
		{
			Util.pf("---------------------------\n");
			
			Util.pf("Repairing file \norig:\t%s\nrepr\t%s\n", _origPath, _reprPath);
			
			write2repair();
					
			Util.pf("Size comparison:\norig\t%d bytes\nrepr\t%d bytes\n",
				getOrigHdfsSize(), getRepairedSize());			
			
			if(!_needRepair)
			{
				Util.pf("File does not need to be repaired\n");	
				return;
			}
			
			if(checkOkay())
			{
				Util.pf("Repaired version is okay, swapping ... ");
				doSwap();
				Util.pf(" ... done\n ");
				
			} else {
				
				Util.pf("Error detecting in repair, skipping");
			}
		}
		
		private String generateReprPath()
		{
			long rid = Math.abs(_jRand.nextLong());
			return Util.sprintf("/tmp/repair/rep_%d_%s", rid, _origPath.getName());

		}
		
		private void write2repair() throws IOException
		{
			int linecount = 0;
			FileSystem fsys = FileSystem.get(new Configuration());
			
			BufferedWriter bwrite = HadoopUtil.getGzipWriter(fsys, _reprPath);
			BufferedReader bread = HadoopUtil.getGzipReader(fsys, _origPath);
			
			while(true)
			{
				String readline = null;
				
				try { readline = bread.readLine(); }
				catch (IOException ioex) {
					
					// This is normal
					Util.pf("Found exception after line %d\n", linecount);
					break;
				}
				
				if(readline == null)
				{
					// This is actually the exceptional condition
					_needRepair = false;
					Util.pf("Found regular EOF after %d lines\n", linecount);
					break; 
				}
				
				
				bwrite.write(readline);
				bwrite.write("\n");
				
				linecount++;
			}
			
			bwrite.close();
			bread.close();
		}		
		
		private void doSwap() throws IOException
		{
			_fSystem.delete(_origPath, false);
			_fSystem.rename(_reprPath, _origPath);
		}
		
	}
}
