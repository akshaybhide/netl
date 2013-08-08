
package com.adnetik.shared;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.Util.*;

public class HadoopTools 
{	

	static void wildCardBounce(String locpatt, String locpref, String hadpref) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		List<Path> loclist = HadoopUtil.getGlobPathList(fsys, "file:/" + locpatt);
		Util.pf("Found %d paths on local machine\n", loclist.size());
		
		for(Path onepath : loclist)
		{
			Util.pf("One path is %s\n", onepath);	
		}
	}
	

	
	public static class PathListReader implements com.adnetik.shared.Util.LineReader
	{
		BufferedReader _curReader = null;
		
		List<Path> _pathList = Util.vector();
		
		private int _curPathId = 0;
		private FileSystem _fSystem;
		
		public PathListReader(Collection<Path> pcol, FileSystem fsys)
		{
			_pathList.addAll(pcol);	
			_fSystem = fsys;
		}
		
		// Return null if we are done reading from ALL the files
		public String readLine() throws IOException
		{
			if(_curReader == null)
			{
				if(_curPathId < _pathList.size())
				{
					Util.pf("Opening HDFS path %s\n", _pathList.get(_curPathId));
					_curReader = HadoopUtil.hdfsBufReader(_fSystem, _pathList.get(_curPathId));
				} else {
					return null;	
				}
			}
			
			String oneline = _curReader.readLine();
			
			if(oneline == null)
			{
				_curReader.close();
				_curReader = null;
				_curPathId++;
				return readLine();
			}
			
			return oneline;
		}
		
		public int getPathCount()
		{
			return _pathList.size();	
		}
		
		public Path getCurPath()
		{
			return (_curPathId < _pathList.size() ? _pathList.get(_curPathId) : null);
		}
		
		public void close() throws IOException
		{
			if(_curReader != null)
			{ 
				_curReader.close();
				_curReader = null;
			}
		}
	}
	
	// Ignore the key, just output the values.
	public static class JustValueReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		private RecordCountPinger _recPinger = new RecordCountPinger();
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			while(values.hasNext())
			{
				_recPinger.ping(reporter);
				
				collector.collect(values.next(), new Text(""));
			}
		}		
	}		
	
	public static class RecordCountPinger
	{
		private int _recCount = 0;
		
		public void ping(Reporter rep)
		{
			_recCount++;
			
			if((_recCount % 10000) == 0)
				{ rep.incrCounter(HadoopUtil.Counter.Ten_K_Rec, 1); }			
		}
	}
	
	public static Map<String, Long> getCounterMap(RunningJob rjob) throws IOException
	{
		Map<String, Long> cmap = Util.treemap();
		Counters jcount = rjob.getCounters();
		
		for(String groupname : jcount.getGroupNames())
		{
			Counters.Group onegroup = jcount.getGroup(groupname);
			
			for(Counters.Counter onecount : onegroup)
				{ cmap.put(onecount.getDisplayName(), onecount.getValue()); }
		}
		return cmap;
	}
	
	public static void counterMap2Mail(RunningJob rjob, SimpleMail logmail) throws IOException
	{
		Map<String, Long> cmap = getCounterMap(rjob);
		for(String countname : cmap.keySet())
		{
			logmail.pf("Counter %s = %d\n", countname, cmap.get(countname));
		}
	}
	
	public static class MaxMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		private Long _maxRecords;
		private Long _curCount = 0L;
		
		@Override
		public void configure(JobConf job)
		{
			try { 
				String maxrec  = job.get("MAX_RECORDS");
				_maxRecords = Long.valueOf(maxrec);
				
			} catch (Exception ex) {
				throw new RuntimeException("Must set the MAX_RECORDS string field to use MaxMapper");
			}
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{			
			if(_curCount >= _maxRecords)
				{ return; }

			String line = value.toString();
			String[] toks = line.split("\t");
			
			String nosubline = Util.joinButFirst(toks, "\t");
			
			BidLogEntry ble = BidLogEntry.getOrNull(LogType.UIndexMinType, LogVersion.UIndexMinVers2, nosubline);
			Util.massert(ble != null, "Bad log entry");			
			
			
			output.collect(new Text(toks[0]), new Text(Util.joinButFirst(toks, "\t")));
			_curCount++;
		}
	}		
	
}
