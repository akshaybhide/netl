package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.analytics.*;	
	
// Use to rebuild a set of HDFS gzip files that are corrupted on the tails.
public class GzipRebuilder
{
	List<String> _pattList = Util.vector();
	
	List<Path> _rebuildList = Util.vector();
	
	FileSystem _fSystem; 
	
	int _okayCount = 0; // files that were already okay
	int _errCount = 0; // ERRORS
	int _rebCount = 0; // rebuilt
	
	SimpleMail _logMail; 
	
	public static void main(String[] args) throws IOException
	{
		List<String> plist = getPatternList();
		GzipRebuilder gzreb = new GzipRebuilder(plist);

		// Util.pf("Found %d paths with pattern %s\nFirst is %s\n", gzreb.numFound(), pattern, gzreb._rebuildList.get(0));
		
		if(Util.checkOkay("Okay to proceed?"))
			{ gzreb.rebuildAll(); }

	}
	
	private static List<String> getPatternList()
	{
		Util.massert((new File("pattern.txt")).exists(), "Pattern file not found");
		List<String> pattlist = FileUtils.readFileLinesE("pattern.txt");
		Util.massert(!pattlist.isEmpty(), "Found no patterns in file");
		return pattlist;
	}
	
	public GzipRebuilder(List<String> patt) throws IOException
	{
		_logMail = new SimpleMail("GzipRebuilderReport");
		
		_pattList.addAll(patt);

		_fSystem = FileSystem.get(new Configuration());
		
		generateList();
		
	}
	
	public void generateList() throws IOException
	{
		for(String pattern : _pattList)
		{
			List<Path> newpath = HadoopUtil.getGlobPathList(_fSystem, pattern);	
			_logMail.pf("Found %d files for pattern %s\n", newpath.size(), pattern);
			_rebuildList.addAll(newpath);
		}
		
		//bCollections.shuffle(_rebuildList);
	}
	
	public int numFound()
	{
		return _rebuildList.size();	
	}
	
	public void rebuildAll() throws IOException
	{
		double startup = Util.curtime();
		
		for(int i = 0; i < _rebuildList.size(); i++)
		{
			Path toreb = _rebuildList.get(i);	
			rebuildPath(toreb);
			
			String estcomplete = TimeUtil.getEstCompletedTime((i+1), _rebuildList.size(), startup);
			_logMail.pf("Finished rebuilding path %d/%d, estcomplete %s\n",
				(i+1), _rebuildList.size(), estcomplete);
		}
	
		_logMail.pf("Finished rebuild for pattern %s, okay=%d, rebuilt=%d, ERROR=%d\n",
			_pattList.toString(), _okayCount, _rebCount, _errCount);
		
		_logMail.send2admin();
	}
	
	public void rebuildPath(Path torebuild) throws IOException
	{
		Util.massert(torebuild.toString().endsWith(".gz"), 
			"Found path %s, this code only works for GZip files", torebuild);			
		
		int lcount = 0;
		Path gimp = new Path(torebuild.toString() + "__gimp");
		
		BufferedReader bread = HadoopUtil.getGzipReader(_fSystem, torebuild);
		BufferedWriter bwrite = HadoopUtil.getGzipWriter(_fSystem, gimp);
		
		boolean corrupt = false;
		boolean gimpokay = false;
		
		try {
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				bwrite.write(oneline);
				bwrite.write("\n");
				lcount++;
			} 
		} catch (IOException ioex) {
			
			// _logMail.pf("Found data corruption after line %d\n", lcount);
			corrupt = true;
			
			
		} finally {
			
			try { bread.close(); }
			catch (IOException ioex) { }
			
			try { 
				bwrite.close(); 
				// _logMail.pf("Successfully closed writer\n");
				gimpokay = true;
			}
			catch (IOException ioex) {} 			
		}
		
		if(!corrupt)
		{ 
			_logMail.pf("No corruption found for file %s\n", torebuild.toString());
			_okayCount++;
			
		} else {
			
			if(gimpokay)
			{ 
				swapOut(torebuild, gimp); 
				_logMail.pf("Rebuilt file %s, %d lines okay\n", torebuild.toString(), lcount);
				_rebCount++;
			} else {

				_logMail.pf("ERROR: corruption found for file %s, but problem with gimp file, skipping\n",
					torebuild.toString());
				
				_errCount++;
			}
		}
	}
	
	private void swapOut(Path oldpath, Path newpath) throws IOException
	{
		Util.massert(newpath.toString().endsWith("gimp"), 
			"Bad gimp path %s", newpath);
		
		_fSystem.delete(oldpath, false);
		_fSystem.rename(newpath, oldpath);
		
		// _logMail.pf("Deleted old file, renamed old\n");
	}
	
}
