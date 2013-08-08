
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


import com.adnetik.shared.*;
import com.adnetik.userindex.*;

public class AidxPerfExam
{		
	
	private int _totCount = 0;
	private int _hitCount = 0;
	
	private BufferedWriter _curWriter;
	
	private FileSystem _fSystem;
	
	public static void main(String[] args) throws IOException
	{
		
		AidxPerfExam apex = new AidxPerfExam();
		apex.grabBlockData("2013-04-14");
		
	}
	
	AidxPerfExam() throws IOException
	{	
		_fSystem = FileSystem.get(new Configuration());		
	}
	
	
	void grabBlockData(String blockend) throws IOException
	{
		UserIndexUtil.assertValidBlockEnd(blockend);

		_curWriter = FileUtils.getWriter("perfexamdata.txt");
		
		for(int n = 0; n < 7; n++)
		{
			String probeday = TimeUtil.nDaysBefore(blockend, n);
			grabPartFile4Day(probeday);
		}
		
		_curWriter.close();		
	}
	
	void grabPartFile4Day(String daycode) throws IOException
	{
		String partpath = Util.sprintf("/userindex/sortscrub/%s/part-00022.txt.gz", daycode);
		
		BufferedReader bread = HadoopUtil.getGzipReader(_fSystem, partpath);
		
		double startup = Util.curtime();
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String[] toks = oneline.split("\t");
			
			String wtpid = toks[0];
			
			if(wtpid.toLowerCase().endsWith("3b"))
			{
				_hitCount++;
				_curWriter.write(oneline);
				_curWriter.write("\n");
			}
			
			_totCount++;
			
			if((_totCount % 100000) == 0)
			{
				Util.pf("Done with line %d, %d hits, took %.03f secs\n",
					_totCount, _hitCount, (Util.curtime()-startup)/1000);
				
			}
		}
		
		bread.close();
		
		Util.pf("Finished scan of one file, got %d hits, %d total lines, took %.03f\n",
			_hitCount, _totCount, (Util.curtime() - startup)/1000);		
		
		
	}

}
