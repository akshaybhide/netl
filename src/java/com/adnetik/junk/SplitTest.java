
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class SplitTest
{	
	Map<Integer, PrintWriter> _writeMap = Util.treemap();
		
	public static void main(String[] args) throws IOException
	{
		/*
		SplitTest stest = new SplitTest();
		stest.initWriters();
		stest.runScan();
		*/
		
		/*
		LineReader basic = FileUtils.bufRead2Line(FileUtils.getReader("masterhead.txt"));
		LineReader hadline;
		{
			FileSystem fsys = FileSystem.get(new Configuration());
			List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, "/home/burfoot/thirdparty/splittest/*.txt");
			hadline = new HadoopTools.PathListReader(pathlist, fsys);
		}
		
		Map<String, Integer> amap = getIdCountMap(hadline);
		Map<String, Integer> bmap = getIdCountMap(basic);
		
		Util.massert(amap.equals(bmap), "Mismatch");		
		
		Util.pf("Found %d entries for amap, %d entries for bmap\n",
			amap.size(), bmap.size());
		
		basic.close();
		hadline.close();
		*/
		FileSystem fsys = FileSystem.get(new Configuration());
		LineReader basic = FileUtils.bufRead2Line(HadoopUtil.hdfsBufReader(fsys, "/thirdparty/bluekai/MASTER_LIST_2012-10-16.txt"));
		LineReader smart = hdfsGlobLineReader("/thirdparty/bluekai/snapshot/testme/part-*");
		
		testSegData(basic, smart);
		
	}
	
	private static void testSegData(LineReader aline, LineReader bline) throws IOException
	{
		SegmentDataQueue aq = new SegmentDataQueue(aline);
		SegmentDataQueue bq = new SegmentDataQueue(bline);
		
		int lcount = 0;
		
		while(aq.hasNext())
		{
			Map.Entry<String, List<String>> a_entry = aq.nextEntry();
			Map.Entry<String, List<String>> b_entry = bq.nextEntry();
			
			
			Util.massert(a_entry.getKey().equals(b_entry.getKey()),
				"Key mismatch");
			
			Util.massert(a_entry.getValue().size() == b_entry.getValue().size(),
				"LineCount mismatch a=%d, b=%d", a_entry.getValue().size(), b_entry.getValue().size());
			
			lcount++;
			
			if((lcount % 100000) == 0)
			{
				Util.pf("Read %d lines\n", lcount);	
			}
		}
		
		
	}
	
	private static LineReader hdfsGlobLineReader(String pathpatt) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, pathpatt);
		HadoopTools.PathListReader hadline = new HadoopTools.PathListReader(pathlist, fsys);
		Util.pf("Built linereader with %d paths\n", hadline.getPathCount());
		return hadline;
	}
	
	
	private void runScan() throws IOException
	{
		Random jrand = new Random();
		BufferedReader bread = FileUtils.getReader("masterhead.txt");
		int lcount = 0;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			int rid = jrand.nextInt(_writeMap.size());
			_writeMap.get(rid).write(oneline+"\n");
			
			if((lcount++ % 10000) == 0)
			{
				Util.pf("Finished with row %d\n", lcount);
				
			}
		}
		
		for(PrintWriter pwrite : _writeMap.values())
			{ pwrite.close(); }
		
		bread.close();		
	}
	
	private void initWriters() throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		
		for(int i = 0; i < 100; i++)
		{
			PrintWriter pwrite = HadoopUtil.getHdfsWriter(fsys, getTestPath(i));
			
			_writeMap.put(i, pwrite);
		}
	}
	
	private String getTestPath(int pid)
	{
		return Util.sprintf("/home/burfoot/thirdparty/splittest/testpath_%d.txt", pid);
		
	}
	
	private static Map<String, Integer> getIdCountMap(LineReader lread) throws IOException
	{
		Map<String, Integer> countmap = Util.treemap();
		
		for(String oneline = lread.readLine(); oneline != null; oneline = lread.readLine())
		{
			String[] wtp_junk = oneline.trim().split("\t");	
			String wtp = wtp_junk[0];
			Util.incHitMap(countmap, wtp);
		}
		return countmap;	
	}
}
