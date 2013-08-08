
package com.adnetik.shared;

import java.util.*;
import java.io.*;
import java.util.zip.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import java.text.SimpleDateFormat;

import com.adnetik.shared.Util.*;

public class HadoopUtil 
{	 
	public static final Text TEXT_ONE = new Text("1");
	
	public static final int BIG_JOB_POLITE_NODES = 24;
	public static final int MAX_REDUCER_NODES = 30;
	
	public static final String MAGIC_PATH = "/home/burfoot/MAGIC.txt";
	
	public enum Counter { FileTargMismatch, DbSliceLookupTimeSecs, GenericError, ProcUsers, 
		Ten_K_Rec, BadWtpIdRecords,
		PixLogFormatExceptions,
		FoundBlueUsers, FoundExelUsers, // Number of users for whom we found EX or BK data
		InitCount, // Number of times we've called explicit initialization commands, use to check if JVM reuse is working
		ReducerExceptions, ConfigureTime, PathChecks, LogFormatExceptions, NoCurrencyRecords };
	
	public static String getHdfsLzoPath(ExcName exc, LogType ltype, String daycode, LogVersion lvers)
	{
		Util.massert(ltype == LogType.click || ltype == LogType.conversion || ltype == LogType.imp,
			"This method only valid for I/C/C files, called on %s", ltype);
		
		return Util.sprintf("/data/%s/%s_%s_%s.lzo", ltype, exc, daycode, lvers);
	}
	
	public static Path findHdfsLzoPath(FileSystem fsys, ExcName exc, LogType ltype, String daycode) throws IOException
	{
		String patt = Util.sprintf("/data/%s/%s_%s*.lzo", ltype, exc, daycode);
		List<Path> pathlist = getGlobPathList(fsys, patt);
		Util.massert(pathlist.size() <= 1, "Multiple paths found for same logtype/excname/daycode");
		return pathlist.isEmpty() ? null : pathlist.get(0);
	}
	
	
	public static List<Path> pathListFromClArg(Configuration conf, String argz) throws IOException
	{
		if(argz.endsWith(".mani"))
		{
			return HadoopUtil.readPathsFromFile(argz);			
		} else {
			return HadoopUtil.getGlobPathList(conf, argz);
		}	
	}
	
	public static List<Path> extraClPathArgs(Configuration conf, Map<String, String> optArgs) throws IOException
	{
		List<Path> plist = Util.vector();
		
		for(String optkey : optArgs.keySet())
		{
			if(optkey.startsWith("extrainput"))
				{ plist.addAll(pathListFromClArg(conf, optArgs.get(optkey))); }
		}
		
		return plist;
	}	
	
	public static List<Path> removeClPathArgs(Configuration conf, Map<String, String> optArgs) throws IOException
	{
		List<Path> plist = Util.vector();
		
		for(String optkey : optArgs.keySet())
		{
			if(optkey.startsWith("removeinput"))
				{ plist.addAll(pathListFromClArg(conf, optArgs.get(optkey))); }
		}
		
		return plist;
	}		
	
	
	public static List<Path> findLzoFiles(FileSystem fsys, ExcName excName, LogType logType, String dayCode) throws IOException
	{
		return findLzoSub(fsys, (excName == null ? "*" : excName.toString()), 
					(logType == null ? "*" : logType.toString()),
					(dayCode == null ? "*" : dayCode.toString()));
	}

	
	
	
	private static List<Path> findLzoSub(FileSystem fsys, String excName, String logType, String dayCode) throws IOException
	{
		List<Path> plist = Util.vector();
		
		String pathPattern = Util.sprintf("/data/%s/%s_%s.lzo", logType, excName, dayCode);
		
		FileStatus[] filestats = fsys.globStatus(new Path(pathPattern));
		
		if(filestats == null)
			{ return plist;}
		
		for(FileStatus fstat : filestats)
		{
			plist.add(fstat.getPath());
		}
		
		return plist;		
		
	}
	
	public static String getValidOutputPath(Configured configured, String pref) throws IOException
	{
		return getValidOutputPath(FileSystem.get(configured.getConf()), pref);
	}
	
	public static String getValidOutputPath(FileSystem fsys, String pref) throws IOException
	{
		for(int i = 0; i < 100; i++)
		{
			Path p = new Path(pref + i);
			if(!fsys.exists(p))
				{ return pref+i; } 
		}
		
		return null;
	}
	
	public static Path gimmeTempDir(FileSystem fsys) throws IOException
	{
		Random r = new Random();
		
		for(int i = 0; i < 50; i++)
		{
			long probe = r.nextLong();
			probe = (probe < 0 ? -probe : probe);
			
			Path p = new Path(Util.sprintf("/tmp/gimp_%d", probe));

			if(!fsys.exists(p))
				{ return p; }
		}
		
		Util.pf("\nError: no valid temp dirs found after 50 tries!!! \nClean up the /tmp dir!!");
		throw new RuntimeException("No valid temp dir found");
	}
	
	public static void collapseDirCleanup(FileSystem fsys, Path srcdir, Path trgfile) throws IOException
	{
		Util.pf("\nCollapsing from %s to %s", srcdir, trgfile);
	
		// Autodetect extension		
		String gpattern = Util.sprintf("%s/part-00000*", srcdir);
		List<Path> pathlist = getGlobPathList(fsys, gpattern);
		
		if(pathlist.size() == 0)
			{ throw new RuntimeException("No files found matching pattern " + gpattern); }
		
		Path partpath = pathlist.get(0);
		
		// make sure parents exists
		fsys.mkdirs(trgfile.getParent());
		
		// Hard rename - fuck this "ask before overwriting" hadoopy bullshit
		fsys.delete(trgfile, true);		
		
		// Move
		fsys.rename(partpath, trgfile);
		
		// Clean up the original dir
		fsys.delete(srcdir, true);
	}
	
	public static Path getUserSetPath(String daycode)
	{
		String s = Util.sprintf("/userindex/sortscrub/%s/part-00028.lzo", daycode);
		return new Path(s);
	}
	
	public static void stripDirToFile(FileSystem fsys, Path outputdir, String ext) throws IOException
	{
		Path partpath = new Path(Util.sprintf("%s/part-00000%s", outputdir, (ext == null ? "" : "." + ext)));
		Path filepath = new Path(Util.sprintf("%s%s", outputdir, (ext == null ? ".txt" : "." + ext)));
		
		// Hard rename
		fsys.delete(filepath, true);
		
		// Rename part file to plain file
		fsys.rename(partpath, filepath);
		
		// delete the directory
		fsys.delete(outputdir, true);
	}

	public static void writeLinesToPath(Collection<String> lines, FileSystem fsys, Object pathOrString) throws IOException
	{
		OutputStream fos = fsys.create(new Path(pathOrString.toString()));
		
		for(String oneline : lines)
			{ fos.write((oneline + "\n").getBytes()); }
	
		fos.close();		
	}
	
	public static void writeObjectStr2Path(Collection<? extends Object> lines, FileSystem fsys, Object pathOrString) throws IOException
	{
		OutputStream fos = fsys.create(new Path(pathOrString.toString()));
		
		for(Object oneline : lines)
			{ fos.write((oneline.toString() + "\n").getBytes()); }
	
		fos.close();		
	}	
	
	

	
	public static String smartOutputPath(Configured confg, String jobcode) throws IOException
	{
		String curDir = System.getProperty("user.dir");
		String pref = Util.sprintf("%s/%s", curDir, jobcode);
		return getValidOutputPath(confg, pref);
	}
	
	public static String smartRemovePath(Configured confg, String jobcode) throws IOException
	{
		String curDir = System.getProperty("user.dir");
		String path = Util.sprintf("%s/%s", curDir, jobcode);

		checkRemovePath(confg, path);
		return path;
	}
	
	// 
	public static Path classSpecTempPath(Object o)
	{
		String specpath = "/tmp/" + o.getClass().getSimpleName();
		return new Path(specpath);
	}
	
	public static void checkRemovePath(Configured confg, Object pathOrString) throws IOException
	{
		FileSystem fsys = FileSystem.get(confg.getConf());		
		checkRemovePath(fsys, pathOrString);
	}
	
	public static void checkRemovePath(FileSystem fsys, Object pathOrString) throws IOException
	{
		String pathToCheck = pathOrString.toString();		
		
		if(fsys.exists(new Path(pathToCheck)))
		{
			System.out.printf("\nDelete Output Path %s ? [yes/NO] ", pathToCheck);			
			
			Scanner sc = new Scanner(System.in);
			String input = sc.nextLine();
			sc.close();
			
			if(!"yes".equals(input))
			{ 
				System.out.printf("\nPath not deleted, terminating.");
				System.exit(1);
			}
		}
		
		fsys.delete(new Path(pathToCheck), true);		
		
		
	}
	
	public static void moveToTrash(Configured confg, Path rpath) throws IOException
	{
		moveToTrash(FileSystem.get(confg.getConf()), rpath);	
	}
	
	public static void moveToTrash(FileSystem fsys, Path rpath) throws IOException
	{
		if(!fsys.exists(rpath))
		{
			Util.pf("\nFile does not exist %s", rpath.toString());	
			return;
		}
		
		int origind = 1;
		Path trashpath = null;
		
		for(int i = 0; i < 100; i++)
		{
			String jpath = "/trash/" + rpath.toString().replaceAll("/", "_") + "_" + i;

			if(!fsys.exists(new Path(jpath)))
			{
				trashpath = new Path(jpath);
				Util.pf("Orig/trash path is \n\t%s\n\t%s\n", rpath, trashpath);
				break; 				
			}
		}
		
		if(trashpath == null)
		{
			Util.pf("\nCound not find valid trash path for orig path %s, exiting", rpath.toString());
			System.exit(1);
		}
		
		fsys.rename(rpath, trashpath);
	}
	

	
	public static Set<String> getExtensionSet(Collection<Path> pathlist)
	{
		Set<String> extset = Util.treeset();
		
		for(Path p : pathlist)
		{
			String[] toks = p.toString().split("\\.");
			extset.add(toks[toks.length-1]);
		}
		
		return extset;		
	}
	
	// TODO: put all of these mappers and reducers in their own file
	public static class EmptyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		private int _recCount;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{			
			_recCount++;
			
			if((_recCount % 10000) == 0)
			{ 
				// TODO: is the progress() call redundant with the counter-inc?
				reporter.incrCounter(HadoopUtil.Counter.Ten_K_Rec, 1); 
				reporter.progress(); // 
			}
			
			String line = value.toString();
			String[] toks = line.split("\t");
			output.collect(new Text(toks[0]), new Text(Util.joinButFirst(toks, "\t")));
		}
	}	
	
	// TODO: put all of these mappers and reducers in their own file
	public static class CheckWtpEmptyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		private int _recCount;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{			
			_recCount++;
			
			if((_recCount % 10000) == 0)
			{ 
				// TODO: is the progress() call redundant with the counter-inc?
				reporter.incrCounter(HadoopUtil.Counter.Ten_K_Rec, 1); 
				reporter.progress(); // 
			}
			
			String line = value.toString();
			String[] toks = line.split("\t");
			
			// Skip records with badly formatted WTP ID
			WtpId widkey = WtpId.getOrNull(toks[0].trim());
			if(widkey == null)
			{ 
				reporter.incrCounter(HadoopUtil.Counter.BadWtpIdRecords, 1);
				return; 
			}
			
			output.collect(new Text(widkey.toString()), new Text(Util.joinButFirst(toks, "\t")));
		}
	}		
	
	
	// TODO: figure out how to use Hadoop code to do this
	public static class EmptyReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			while(values.hasNext())
			{
				collector.collect(key, values.next());
			}
		}		
	}	
	
	
	
	
	public static class SetReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		SortedSet<String> checkSet = Util.treeset();
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{			
			checkSet.clear();
			
			while(values.hasNext())
			{
				checkSet.add(values.next().toString());	
			}
			
			for(String line : checkSet)
			{
				collector.collect(key, new Text(line));
			}
		}		
	}	

	public static class CountReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, LongWritable>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,LongWritable> collector, Reporter reporter) 
		throws IOException
		{
			long count = 0;
			
			
			while(values.hasNext())
			{
				long x = Long.valueOf(values.next().toString());	
				count += x;
			}
			
			//collector.collect(key, new LongWritable(maxVal));
			collector.collect(key, new LongWritable(count));
		}		
	}	
	
	public static class DoubleCountReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{
			double count = 0;
			
			while(values.hasNext())
			{
				double x = Double.valueOf(values.next().toString());	
				count += x;
			}
			
			//collector.collect(key, new LongWritable(maxVal));
			collector.collect(key, new Text("" + count));
		}		
	}		

	public static class SetCountReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, LongWritable>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,LongWritable> collector, Reporter reporter) 
		throws IOException
		{
			Set<String> x = Util.treeset();			
			
			while(values.hasNext())
			{
				x.add(values.next().toString());
			}
			
			//collector.collect(key, new LongWritable(maxVal));
			collector.collect(key, new LongWritable(x.size()));
		}		
	}	
		
	// Useful for generating part-000xx strings
	public static String pad5(int topad)
	{
		String x = "" + topad;
		
		while(x.length() < 5)
			{ x = "0" + x; }
		
		return x;
	}	
		
	public static Path[] getGlobPaths(Configuration conf, String pattern) throws IOException
	{
		Path pathPattern = new Path(pattern);
		
		FileSystem fsys = FileSystem.get(conf);
		
		// Argh this is a one-liner in Python
		FileStatus[] fstats = fsys.globStatus(pathPattern);
		
		Path[] globpaths = new Path[fstats.length];
		
		for(int i = 0; i < fstats.length; i++)
		{
			globpaths[i] = fstats[i].getPath();	
		}
		
		return globpaths;
	}		
	
	public static SortedMap<String, SortedSet<Path>> getDateSortedPathMap(FileSystem fsys, String pattern) throws IOException
	{
		SortedMap<String, SortedSet<Path>> pathmap = Util.treemap();
		
		for(Path p : getGlobPathList(fsys, pattern))
		{
			String daycode = RegexpUtil.findDayCode(p.toString());
			
			Util.massert(daycode != null, 
				"Could not find ISO daycode in path %s", p);
			
			Util.setdefault(pathmap, daycode, new TreeSet<Path>());
			pathmap.get(daycode).add(p);
		}
		
		return pathmap;
	}
	
	
	public static List<Path> getGlobPathList(FileSystem fsys, String pattern) throws IOException
	{
		List<Path> pathlist = Util.vector();
		
		FileStatus[] statarr = fsys.globStatus(new Path(pattern));
	
		if(statarr != null)
		{			
			for(FileStatus fstat : statarr)
				{ pathlist.add(fstat.getPath()); }	
		}
		
		return pathlist;		
	}
	
	public static void checkFileWritePerm(FileSystem fsys, Path targfilepath) throws IOException
	{
		List<String> gimplines = Util.vector();
		gimplines.add("gimp");
		
		
		// Write and delete
		HadoopUtil.writeLinesToPath(gimplines, fsys, targfilepath);		
		fsys.delete(targfilepath, false);
	}
	
	public static void checkDirWritePerm(FileSystem fsys, Path outputDir) throws IOException
	{		
		if(fsys.exists(outputDir))
			{ throw new RuntimeException("Path already exists: " + outputDir); }

		try {
			fsys.mkdirs(outputDir);
			
			String gimptarg = Util.sprintf("%s/gimp.txt", outputDir);
			
			List<String> gimplines = Util.vector();
			gimplines.add("gimp");
			
			HadoopUtil.writeLinesToPath(gimplines, fsys, gimptarg);
			
			fsys.delete(outputDir, true);

		} catch (Exception ex) {
			
			Util.pf("\nWrite operation to directory %s failed, check permissions", outputDir);
			Util.pf("\nException is %s\n", ex.getMessage());
			System.exit(1);
		}
		
		Util.pf("\nPermission check passed for dir %s\n", outputDir);
		
	}
	
	public static int runEnclosingClass(String[] args) throws Exception
	{
		Throwable t = new Throwable();
		StackTraceElement[] stelem = t.getStackTrace();
		String callclass = stelem[1].getClassName();

		Tool torun = null;
		
		try {		
			Util.pf("Running tool class %s\n", callclass);
			
			torun  = Util.cast(Class.forName(callclass).newInstance());
		} catch (Exception ex ) {
			
			Util.pf("\nError instantiating class %s, error %s", callclass, ex.getMessage());	
			return 0;
		}

		return ToolRunner.run(torun, args);
	}
	
	
	public static Map<Path, Long> getGlobPathSizeMap(FileSystem fsys, String pattern) throws IOException
	{
		Map<Path, Long> sizemap = Util.treemap();
		List<Path> pathlist = Util.vector();
		
		for(FileStatus fstat : fsys.globStatus(new Path(pattern)))
		{
			sizemap.put(fstat.getPath(), fstat.getLen());
		}
		
		return sizemap;		
	}	
	
	
	public static List<Path> getPixelLogPaths(Configuration conf, String alpha, String omega, boolean isHdfs)
	throws IOException
	{
		List<String> daylist = TimeUtil.getDateRange(alpha, omega);
		return getPixelLogPaths(conf, daylist, isHdfs);		
	}	
	
	public static List<Path> getPixelLogPaths(Configuration conf, List<String> daylist, boolean isHdfs)
	throws IOException
	{
		List<Path> pathlist = Util.vector();
		
		for(String daycode : daylist)
		{
			if(isHdfs)
			{
				// eg data/pixel/pix_2011-11-13.lzo
				Path p = new Path(Util.sprintf("/data/pixel/pix_%s.lzo", daycode));
				FileSystem fsys = FileSystem.get(conf);
				
				if(fsys.exists(p))
					{ pathlist.add(p);}
				else
					{ Util.pf("\nWarning, pixel file %s does not exist, skipping.", p); }
								
			} else {
				// NFS
				String pathpattern = Util.sprintf("file:///mnt/adnetik/adnetik-uservervillage/prod/userver_log/pixel/%s/*.log.gz", daycode);
				pathlist.addAll(getGlobPathList(conf, pathpattern));
			}
		}
		
		return pathlist;
	}
	
	
	public static PathInfo getNfsPathInfoFromReporter(Reporter rep)
	{
		String path = rep.getInputSplit().toString();
		
		Util.massert(path.indexOf(".log.gz") > -1,
			"Invalid path %s, expected to end with .log.gz");
		
		return new PathInfo(path);
	}
	
	// TODO: need unified treatment for finding logtype/version from paths
	public static Object[] logTypeVersionFromReporter(Reporter rep)
	{
		String path = rep.getInputSplit().toString();
		
		Util.pf("\nPath string is %s", path);
		
		if(path.indexOf(".log.gz") > -1)
		{
			// Okay, this is a regular log file, either on NFS or HDFS,
			// it doesn't matter.
			try {
				PathInfo pinf = new PathInfo(path);
				return new Object[] { pinf.pType, pinf.pVers };
			} catch (Exception ex) {
				return null;	
			}
		}
		
		// Try testing as a /data/<logtype>/<excname>_<daycode>.lzo
		{
			RegexpUtil.HdfsPathBag hpb = RegexpUtil.getPathBag(path);
			if(hpb != null)
				{ return new Object[] { hpb.ltype, hpb.lvers }; }
		}
		
		return null; 
	}
	
	public static LogVersion fileVersionFromReporter(Reporter rep)
	{
		String path = rep.getInputSplit().toString();
		return Util.fileVersionFromPath(path);		
	}
	
	public static LogVersion targetVersionFromReporter(Reporter rep)
	{
		String path = rep.getInputSplit().toString();
		String daycode = Util.findDayCode(path);
		return Util.targetVersionFromDayCode(daycode);		
	}	
	
	public static String getHdfsLzoPixelPath(String daycode)
	{
		return Util.sprintf("/data/pixel/pix_%s.lzo", daycode);
	}
	
	
	public static List<Path> getGlobPathList(Configuration conf, String pattern) throws IOException
	{				
		FileSystem fsys = (pattern.startsWith("file://") ? 
			FileSystem.getLocal(conf) : FileSystem.get(conf));
		
		return getGlobPathList(fsys, pattern);
	}

	public static void deleteIfPresent(FileSystem fsys, Object pathOrString) throws IOException
	{
		Path todel = new Path(pathOrString.toString());
		
		if(fsys.exists(todel))
		{
			Util.pf("Previous file %s exists, overwriting\n", todel);
			fsys.delete(todel, false);
		}		
	}
	
	public static List<Path> readPathsFromFile(String inputFile)
	{
		List<Path> pathlist = Util.vector();
		
		for(String s : FileUtils.readFileLinesE(inputFile))
		{
			pathlist.add(new Path(s));	
		}
		// this is a new comment.
		return pathlist;
	}
	
	public static List<String> readFileLinesE(Object pathOrString)
	{
		return readFileLinesE(new Configuration(), pathOrString);	
	}
	
	public static List<String> readFileLinesE(Configuration conf, Object pathOrString)
	{
		try { return readFileLinesE(FileSystem.get(conf), pathOrString); }
		catch (Exception ex ) {
			throw new RuntimeException(ex);	
		}
	}
	
	public static List<String> readFileLinesE(FileSystem fsys, Object pathOrString)
	{
		try {
			Path p = (pathOrString instanceof Path ? (Path) pathOrString : new Path(pathOrString.toString()));
			
			Scanner sc = new Scanner(fsys.open(p));
			
			List<String> slist = Util.vector();
			
			while(sc.hasNextLine()) 
			{ 
				String oneline = sc.nextLine().trim();

				if(oneline.length() > 0)
					{ slist.add(oneline); }
			}
			
			return slist;
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);	
		}
	}	
	
	public static BufferedWriter getGzipWriter(FileSystem fsys, Object pathOrString) throws IOException
	{
		return getGzipWriter(fsys, pathOrString, BidLogEntry.BID_LOG_CHARSET);
	}	

	public static BufferedWriter getGzipWriter(FileSystem fsys, Object pathOrString, String charsetName) throws IOException
	{
		OutputStream fos = fsys.create(new Path(pathOrString.toString()));
		GZIPOutputStream gzos = new GZIPOutputStream(fos);
		return new BufferedWriter(new OutputStreamWriter(gzos, charsetName));
	}
	
	public static PrintWriter getHdfsWriter(FileSystem fsys, Object pathOrString) throws IOException
	{
		OutputStream fos = fsys.create(new Path(pathOrString.toString()));
		PrintWriter pw = new PrintWriter(fos);
		return pw;
	}	
	
        public static BufferedReader getGzipReader(FileSystem fsys, Object pathOrString, String encoding) throws IOException
        {
        	InputStream filestream = fsys.open(new Path(pathOrString.toString()));
               	InputStream gzipstream = new GZIPInputStream(filestream);
        	return new BufferedReader(new InputStreamReader(gzipstream, encoding));
        }
               
        public static BufferedReader getGzipReader(FileSystem fsys, Object pathOrString) throws IOException
        {
        	return getGzipReader(fsys, pathOrString,  BidLogEntry.BID_LOG_CHARSET);
        }	
        
	public static BufferedReader hdfsBufReader(FileSystem fsys, Object pathOrString)
	{
		return hdfsBufReader(fsys, pathOrString, BidLogEntry.BID_LOG_CHARSET);
	}
	
	public static BufferedReader hdfsBufReader(FileSystem fsys, Object pathOrString, String charEncoding)
	{
		try {
			Path p = (pathOrString instanceof Path ? (Path) pathOrString : new Path(pathOrString.toString()));
			BufferedReader bread = new BufferedReader(new InputStreamReader(fsys.open(p), charEncoding)); 	
			return bread;	
			
		} catch (Exception ex) {
			throw new RuntimeException(ex);	
		}				
	}	
	
	public static Scanner hdfsScanner(FileSystem fsys, Object pathOrString) throws IOException
	{
		Path p = (pathOrString instanceof Path ? (Path) pathOrString : new Path(pathOrString.toString()));
		return new Scanner(fsys.open(p));
	}		

	
	public static Integer checkCorrupt(FileSystem fsys, Object pathOrString)
	{
		Integer lcount = 0;
		
		try {
			Util.massert(fsys.exists(new Path(pathOrString.toString())), "Path does not exist: %s", pathOrString);
			
			BufferedReader bread = hdfsBufReader(fsys, pathOrString);
			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
				{ lcount++;	}
			
			bread.close();

		} catch (IOException ioex) {
			
			return null;	
		}

		return lcount;		
	}
	


	
	@SuppressWarnings("unchecked")
	public static <T> T unserialize(FileSystem fsys, Path objpath) throws Exception
	{
		InputStream hadin  = fsys.open(objpath);
		return (T)FileUtils.unserialize(hadin);
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T unserializeEat(FileSystem fsys, Path objpath)
	{
		try {
			InputStream hadin  = fsys.open(objpath);
			return (T)FileUtils.unserialize(hadin);
		} catch (Exception ex) {
			throw new RuntimeException(ex);	
		}
	}
	
	public static void serialize(Serializable tosave, FileSystem fsys, Object  pathOrString) throws IOException
	{
		OutputStream hdfsout = fsys.create(new Path(pathOrString.toString()));
		FileUtils.serialize(tosave, hdfsout);
		hdfsout.close();
	}
	
	public static void serializeEat(Serializable tosave, FileSystem fsys, Object pathOrString)
	{
		try { serialize(tosave, fsys, pathOrString); }
		catch (IOException ioex)  {
			throw new RuntimeException(ioex);	
		}
	}	
	
	
	public static void setLzoOutput(Configured cfgrd)
	{
		cfgrd.getConf().setBoolean("mapred.output.compress", true);
		cfgrd.getConf().setClass("mapred.output.compression.codec", LzopCodec.class, CompressionCodec.class);
	}

	public static void checkSendFailMail(RunningJob jobrun, Configured jobcaller) throws IOException
	{
		checkSendFailMail(jobrun.isSuccessful(), jobcaller);
	}
	
	public static void checkSendFailMail(boolean success, Configured jobcaller) throws IOException
	{
		if(success)
			{ return; }
		
		SimpleMail failmail = new SimpleMail("JobFailReport");
		failmail.addLogLine("Job failed for class: " + jobcaller.getClass().getSimpleName());
		failmail.send2admin();
	}	
	
	public void testHadCall()
	{
		
		Util.pf("\nTest hadoop call");	
	}
	
	public static boolean runHadoopJob(JobConf job)
	{
		boolean success = false;
		try 
		{
			RunningJob jobrun = JobClient.runJob(job);		
			success = jobrun.isSuccessful();
		} catch (Exception ex) {
			success = false;
			ex.printStackTrace();
		}		
		
		return success; 
	}
	
	public static void renamePartitions(FileSystem fsystem, Map<String, Integer> renameMap, Path basedir) throws IOException
	{
		renamePartitions(fsystem, renameMap, basedir, "");
	}
	
	public static void renamePartitions(FileSystem fsystem, Map<String, Integer> renameMap, Path basedir, String suffix) throws IOException
	{
		// TODO: rewrite this to be smarter about extensions
		for(String onecode : renameMap.keySet())
		{
			int partcode = renameMap.get(onecode);
			Path srcpath = new Path(basedir, new Path(Util.sprintf("part-%s", Util.padLeadingZeros(partcode, 5))));
			Path dstpath = new Path(basedir, new Path(onecode+suffix));
			fsystem.rename(srcpath, dstpath);
		}		
	}	
	
	
	public static boolean checkUpDeleteLocal(FileSystem fsys, File localpath, Path hdfspath) throws IOException
	{
		if(!localpath.exists())
		{
			Util.pf("Warning: local file not found: %s", localpath);
			return false; 
		}
		
		if(!fsys.exists(hdfspath))
		{
			Util.pf("Warning: hdfs path not found: %s", hdfspath);
			return false; 
		}
		
		long loclen = localpath.length();
		long dstlen = fsys.getFileStatus(hdfspath).getLen();
		
		if(loclen == dstlen)
		{	
			// We're good - delete the local file
			// Util.pf("HDFS path exactly identical, going to delete local file %s\n", localpath);
			return localpath.delete();
			// return false;
		}
		
		Util.pf("Mismatch local vs. hdfs file lengths:\n\t%d\n\t%d\n", loclen, dstlen);
		return false;
	}
	
	public static <BKEY, BVAL> void  alignMapper(JobConf job, 
			Mapper<LongWritable, Text, BKEY, BVAL> theMap, BKEY bkey, BVAL bval)
	{
		// Null Op! Just a compile test.
	}			

	public static <BKEY, BVAL, CKEY, CVAL> void  alignReducer(JobConf job, 
			Reducer<BKEY, BVAL, CKEY, CVAL> theRed, BKEY bkey, BVAL bval, CKEY ckey, CVAL cval)
	{
		// Null Op! Just a compile test.
	}	
	
	// This provide type safety. Once you get this to compile,
	// you're guaranteed not to get errors at runtime from mismatch between job types.
	public static <BKEY, BVAL, CKEY, CVAL> void  alignJobConf(JobConf job, 
			Mapper<LongWritable, Text, BKEY, BVAL> theMap,
			Reducer<BKEY, BVAL, CKEY, CVAL> theRed,
			BKEY bkey, BVAL bval, CKEY ckey, CVAL cval)
	{
		alignJobConf(job, true, theMap, theRed, null, bkey, bval, ckey, cval);
	}		
	
	public static <BKEY, BVAL, CKEY, CVAL> void  alignJobConf(JobConf job, 
			boolean checkStack,
			Mapper<LongWritable, Text, BKEY, BVAL> theMap,
			Reducer<BKEY, BVAL, CKEY, CVAL> theRed,
			Partitioner<BKEY, BVAL> thePrt,
			BKEY bkey, BVAL bval, CKEY ckey, CVAL cval)
	{
		// Test that both mapper and reducer can be created with zero-argument constructors
		try {
			Object mapcopy = theMap.getClass().newInstance();
			Object redcopy = theRed.getClass().newInstance();
			
			if(thePrt != null)
				{ Object prtcopy = thePrt.getClass().newInstance(); }
			
		} catch (Exception ex) {
			
			Util.pf("\nError: object does not have a zero-argument constructor\n");
			throw new RuntimeException(ex);
		}
		
		// TODO: put this back in...?
		/*
		if(checkStack)
		{
			Throwable t = new Throwable();
			StackTraceElement[] stelem = t.getStackTrace();
			
			String callclass = stelem[1].getClassName();
			String mainclass = stelem[stelem.length-1].getClassName();
			
			if(!mainclass.equals(callclass))
			{
				Util.pf("\nJob alignment error: \n\tcall class is %s\n\tmain class is %s\n", callclass, mainclass);
				System.exit(1);
			}
		}
		*/
		
		
		
		// Set the outputs for the Map
		job.setMapOutputKeyClass(bkey.getClass());
		job.setMapOutputValueClass(bval.getClass());		
		
		// Set the outputs for the Map
		job.setOutputKeyClass(ckey.getClass());
		job.setOutputValueClass(cval.getClass());		

		job.setMapperClass(theMap.getClass());		
		job.setReducerClass(theRed.getClass());	
		
		if(thePrt != null)
			{ job.setPartitionerClass(thePrt.getClass()); }
	}			
}
