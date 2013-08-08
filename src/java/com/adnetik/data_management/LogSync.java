
package com.adnetik.data_management;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;


public class LogSync extends Configured implements Tool
{
	public static final String EMPTY_LZO_PATH = "/mnt/src/java/data_management/emptyfile.lzo";
		
	private String _dayCode; 
	
	private SimpleMail _logMail;
	
	private FileSystem _fSystem; 
	
	// Set of LogVersions we have observed in day's data files. 
	// Usually there is only one, but when version changes happen we will see multiple
	private SortedSet<LogVersion> _verSet;
	
	private Path _outputDir;
	
	// This is a special map used for error checking.
	private SortedMap<Pair<ExcName, LogType>, Integer> _pathCountMap = Util.treemap();
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		_dayCode = args[0];
		_dayCode = (_dayCode.equals("yest") ? TimeUtil.getYesterdayCode() : _dayCode);			
		TimeUtil.assertValidDayCode(_dayCode);
		
		_outputDir = new Path(Util.sprintf("/tmp/logsynctmp_%s", _dayCode));
		
		ArgMap argmap = Util.getClArgMap(args);
		boolean renameonly = argmap.getBoolean("renameonly", false);
		
		_logMail = new SimpleMail("LogSyncReport for " + _dayCode);
		_fSystem = FileSystem.get(getConf());	
		
		HadoopUtil.setLzoOutput(this); 
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		{
			Text a = new Text("");		
			HadoopUtil.alignJobConf(job, true, new LogMapper(), new HadoopTools.JustValueReducer(), 
				new ExcTypePartitioner(), a, a, a, a);
		}
		
		_logMail.pf("\nCalling LogSync %s\n", _dayCode);

		// Specify various job-specific parameters     
		job.setJobName(Util.sprintf("LogSync %s", _dayCode));
		
		List<Path> pathlist = Util.vector();
		
		for(Pair<ExcName, LogType> exclogpair : getExcTypeMap().keySet())
		{
			ExcName excname = exclogpair._1;
			LogType logtype = exclogpair._2;
			
			String nfsdir = Util.getNfsDirPath(excname, logtype, _dayCode);
			String pathpattern = Util.sprintf("file://%s*.log.gz", nfsdir);
			List<Path> singlelist = HadoopUtil.getGlobPathList(getConf(), pathpattern);
			
			_logMail.pf("Found %d paths for ExcName=%s, LogType=%s\n", singlelist.size(), excname, logtype);
			pathlist.addAll(singlelist);
			
			Util.putNoDup(_pathCountMap, Pair.build(excname, logtype), singlelist.size());
		}
		
		addInfo2VerSet(pathlist);
		job.setStrings("TARGET_VERSION", getTargetVersion().toString());
				
		FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));	
		
		// Number of reducers equal to number of elements in this map
		job.setNumReduceTasks(getExcTypeMap().size()); 		
		
		TreeMap<Pair<ExcName, LogType>, Integer> exctypemap = getExcTypeMap();
		
		// Paranoia
		{
			Pair<ExcName, LogType> lastkey = exctypemap.lastKey();
			int lkval = exctypemap.get(lastkey);
			Util.massert(exctypemap.size() == lkval+1, 
				"Error configuring typemap, lastkey=%s, lastval %d, size %d, map %s",
				lastkey, lkval, exctypemap.size(), exctypemap);
			
		}
		Util.massert(exctypemap.get(exctypemap.lastKey())+1 == exctypemap.size(),
			"Error configuring ExcTypeMap");
		
		if(!renameonly)
		{
			// Output path
			// Path outputDir = HadoopUtil.gimmeTempDir(fSystem);
			if(_fSystem.exists(_outputDir))
			{
				_logMail.pf("WARNING: output directory %s already exists, deleting\n", _outputDir);
				_fSystem.delete(_outputDir, true);
			}
		
			// Set the outputdir
			{
				_logMail.pf("\nUsing temp dir %s\n", _outputDir);
				FileOutputFormat.setOutputPath(job, _outputDir);		
			}
			
			// Submit the job, then poll for progress until the job is complete
			RunningJob runJob = JobClient.runJob(job);	
		}
		
		cleanNRename();		
		_logMail.send2admin();
		
		return 0;
	}
	
	private void cleanNRename() throws IOException
	{
		_logMail.pf("Reorganizing output...\n");	
		Map<Pair<ExcName, LogType>, Integer> exctypemap = getExcTypeMap();
		
		Pair<LogType, ExcName> badkey = Pair.build(LogType.imp, ExcName.rtb);
		
		for(Pair<ExcName, LogType> exclogkey : exctypemap.keySet())
		{
			ExcName excname = exclogkey._1;
			LogType logtype = exclogkey._2;
			int reduceid = exctypemap.get(exclogkey);
			// int pathcount = _pathCountMap.get(badkey);
			
			Util.massert(logtype == LogType.imp || logtype == LogType.conversion || logtype == LogType.click,
				"Logtype %s is not supported, must refactor", logtype);
			
			Path srcpath = new Path(Util.sprintf("%s/part-%s.lzo", _outputDir, Util.padLeadingZeros(reduceid, 5)));
			
			boolean nodata = false;
			
			Util.massert(_pathCountMap.containsKey(exclogkey), 
				"PathCountMap is missing key for pair %s", exclogkey);
			
			int pathcount = _pathCountMap.get(exclogkey);
			
			if(!_fSystem.exists(srcpath))
			{ 
				nodata = true;
			}
			
			// This happens if there weren't any records
			// Even a single record should be larger than 50 bytes
			if(_fSystem.getFileStatus(srcpath).getLen() < 50)
			{
				nodata = true;
				_fSystem.delete(srcpath, false); 
			}
			
			if(nodata)
			{ 	
				if(pathcount > 0)
				{
					_logMail.pf("***WARNING***: no records found for ExcName=%s, LogType=%s, but found %d paths\n", 
						excname, logtype, pathcount); 
				} else {
					_logMail.pf("No records found for  ExcName=%s, LogType=%s as expected\n",
						excname, logtype);
				}
				
				continue;
				
			} else if(pathcount == 0) { 

				_logMail.pf("***WARNING***: LZO outputs found for ExcName=%s, LogType=%s, but pathcount=%d\n", 
					excname, logtype, pathcount); 	
				continue;
			}
			
			// Look at all this type safety!!!
			Path dstpath = new Path(HadoopUtil.getHdfsLzoPath(excname,
				logtype, _dayCode, getTargetVersion()));
			
			// Hard rename 
			_fSystem.delete(dstpath, false);
			
			_logMail.pf("Renaming %s --> %s\n", srcpath, dstpath);
			// Rename temp,partfile path to DST path
			_fSystem.rename(srcpath, dstpath);
		}
		
		// Finally, clean up the temp dir itself
		// TODO put this back in, after we have checked that this works
		// _logMail.pf("Deleting tmp dir %s\n", _outputDir);
		// _fSystem.delete(_outputDir, true);		
	}
	
	public static TreeMap<Pair<ExcName, LogType>, Integer> getExcTypeMap()
	{
		TreeMap<Pair<ExcName, LogType>, Integer> emap = Util.treemap();
		
		for(ExcName oneexc : ExcName.values())
		{
			for(LogType ltype : LogType.values())
			{
				// Have to do this somewhat odd method to make sure the lastkey has the highest value
				if(ltype == LogType.imp || ltype == LogType.click || ltype == LogType.conversion)
				{
					Pair<ExcName, LogType> onepair = Pair.build(oneexc, ltype);
					Util.putNoDup(emap, onepair, emap.size());
				}
			}
		}
		
		return emap;
	}
	
	private LogVersion getTargetVersion()
	{
		Util.massert(_verSet != null && !_verSet.isEmpty(),
			"Must initialize verSet before calling");
		
		return _verSet.first();
	}
	
	private void addInfo2VerSet(Collection<Path> pathlist)
	{
		Util.massert(_verSet == null, "VerSet already initialized");
		
		_verSet = Util.treeset();
		
		for(Path onepath : pathlist)
		{
			PathInfo pinf = new PathInfo(onepath.toString());
			_verSet.add(pinf.pVers);
		}		
	}
	
	
	public static class LogMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		// The actual type of the file
		private PathInfo _fileInfo;
		
		// Version we WANT it to be - backtransform if fileVers != targVers;
		private LogVersion _targVers;
		
		Integer targFieldCount;
		
		@Override
		public void configure(JobConf job)
		{
			try { 
				String str_vers  = job.get("TARGET_VERSION");
				_targVers = LogVersion.valueOf(str_vers);
				
				// targFieldCount = FieldNames.getFieldCount(targType, targVers);
				
			} catch (Exception ex) {
				Util.pf("\nError configuring filter");
				throw new RuntimeException(ex);
			}
		}		
		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			// Lookup the version info from the Reporter
			if(_fileInfo == null)
			{ 
				_fileInfo = HadoopUtil.getNfsPathInfoFromReporter(reporter);
			}
			
			try {
				BidLogEntry ble = new BidLogEntry(_fileInfo.pType, _fileInfo.pVers, value.toString());
				
				if(_fileInfo.pVers != _targVers)
					{ ble = ble.transformToVersion(_targVers); }
				
				ble.basicScrub();
				
				// Exchange, LogType, timestamp
				// Need to include timestamp so we sort by time
				String keycodestr = Util.varjoin(Util.DUMB_SEP,
					_fileInfo.pExc, _fileInfo.pType, ble.getField(LogField.date_time));
				
				output.collect(new Text(keycodestr), new Text(ble.getLogLine()));
				
			} catch (BidLogFormatException blex) {
				
				reporter.incrCounter(blex.e, 1);
			}
		}
	}
	
	public static class ExcTypePartitioner implements Partitioner<Text, Text>
	{
		Map<Pair<ExcName, LogType>, Integer> _excTypeMap;
		
		@Override
		public void configure(JobConf jobconf) {}
		
		public int getPartition(Text key, Text value, int numPart)
		{
			// Do this here, as opposed to static initializers, so you get debug info 
			// if something goes wrong
			if(_excTypeMap == null)
			{ 
				_excTypeMap = getExcTypeMap();
			}
			
			String[] toks = key.toString().split(Util.DUMB_SEP);
			Util.massert(toks.length == 3, "Bad combined key %s", key);
			
			// toks[2] is timestamp
			Pair<ExcName, LogType> pairkey = Pair.build(ExcName.valueOf(toks[0]), LogType.valueOf(toks[1]));
			
			return _excTypeMap.get(pairkey);
		}
	}	
	
	
}
