
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.mapred.*;
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;


import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
// import com.adnetik.analytics.InterestUserUpdate;
import com.adnetik.userindex.UserIndexUtil.StagingType;
import com.adnetik.userindex.ScanRequest.*;

// This is basically a small job that generates the 
public class FinalListJob extends Configured implements Tool
{	
	private StatusReportMan _reportMan = new StatusReportMan();
	
	private SimpleMail _logMail;
	
	public enum FinalCounter { NoCountryUsers };
	
	public static final String TARG_DAY_CODE = "TARG_DAY";
	
	public static void main(String[] args) throws Exception
	{
		int mainCode = HadoopUtil.runEnclosingClass(args);
		Util.pf("\n");			
	}

	public int run(String[] args) throws Exception
	{	
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fsystem = FileSystem.get(getConf());
		
		ArgMap optmap = Util.getClArgMap(args);
		String blockend = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
		blockend = optmap.getString("blockend", blockend);
		
		boolean ischart = optmap.getBoolean("chart", false);
		boolean onlydownload = optmap.getBoolean("onlydownload", false);
		Integer maxfile = optmap.getInt("maxfile", Integer.MAX_VALUE);
		
		_logMail = new SimpleMail(UserIndexUtil.AIDX_WEEKLY_CODE + " FinalListJob " + blockend);
		
		_logMail.pf("Running for target day %s\n", blockend);		
		
		if(!onlydownload)
		{
			// align
			{
				Text a = new Text("");			
				LongWritable lw = new LongWritable(0);
				HadoopUtil.alignJobConf(job, true, new DumbScoreMapper(), (ischart ? new ChartReducer() : new HeadReducer()), new ListCodePartitioner(),
					a, a, a, a);
			}
			
			
			{
				// String mypatt = "/userindex/userscores/shard_*/part-00000";
				String mypatt = Util.sprintf("/userindex/userscores/%s/shard_*/part-*", blockend);
				//String mypatt = Util.sprintf("/userindex/userscores/%s/shard_00000/part-*", blockend);			
				List<Path> pathlist = HadoopUtil.getGlobPathList(fsystem, mypatt);
				
				_logMail.pf("Found %d input paths\n", pathlist.size());			
				FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
			}
			
			// Annoyingly need to set this
			job.setStrings(TARG_DAY_CODE, blockend);
			
			Map<String, Integer> partmap = readListPartMap(job);
			Util.massert(partmap.size() > 10, "Part map has size %d", partmap.size());
			_logMail.pf("Found partmap of size %d: \n\t%s\n", partmap.size(), partmap);
			job.setNumReduceTasks(partmap.size()); 
			
			Path outputPath = new Path(Util.sprintf("/userindex/%s/%s", (ischart ? "liftreport" : "finalstep"), blockend));
			HadoopUtil.checkRemovePath(fsystem, outputPath);
			FileOutputFormat.setOutputPath(job, outputPath);			
			
			_logMail.pf("Calling final list creation job\n");
			
			// Specify various job-specific parameters   
			String jobname = Util.sprintf("%s creation for %s", (ischart ? "Lift Report" : "Final List"), blockend);
			job.setJobName(jobname);
			
			HadoopUtil.runHadoopJob(job);
			
			HadoopUtil.renamePartitions(fsystem, partmap, outputPath);
		}
		
		if(ischart)
			{ uploadLift2Db(blockend); }
		else
			{ copyList2Local(blockend); }
		
		_reportMan.flushInfo();
		_logMail.send2admin();
		
		return 0;
	}
	
	public void copyList2Local(String blockend) throws IOException
	{
		FileSystem fsys = FileSystem.get(new Configuration());
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, Util.sprintf("/userindex/finalstep/%s/*", blockend));
		
		for(Path p : pathlist)
		{
			if(p.getName().startsWith("_"))
				{ continue; }
			
			// _logMail.pf("Going to download path %s\n", p.toString());
			List<String> idlist = Util.vector();
			
			BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, p);			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				String[] toks = oneline.split("\t");	
				idlist.add(toks[2]);
			}
			bread.close();
			
			String localpath = UserIndexUtil.getLocalListPath(blockend, p.getName());
			FileUtils.createDirForPath(localpath);
			FileUtils.writeFileLines(idlist, localpath);
			_logMail.pf("Wrote %d lines to file %s\n", idlist.size(), localpath);
			
			_reportMan.reportScoreComplete(p.getName(), idlist.size(), blockend);
		}
	}	
	
	void uploadLift2Db(String daycode) throws IOException
	{
		// compile all the paths into a TSV, then do batch upload
		FileSystem fsystem = FileSystem.get(new Configuration());
		
		String liftpatt = Util.sprintf("/userindex/liftreport/%s/*", daycode);
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsystem, liftpatt);
		BufferedWriter bwrite = FileUtils.getWriter(UserIdxDb.LIFT_DUMP_FILE);
		
		for(Path onepath : pathlist)
		{
			if(onepath.getName().startsWith("_"))
				{ continue; }
			
			_logMail.pf("Reading data from file %s\n", onepath);
			
			PosRequest posreq = ListInfoManager.getSing().getPosRequest(onepath.getName());
			
			Integer reportid = UserIdxDb.lookupCreateRepId(daycode, posreq);
			
			Util.massert(reportid != null, "Could not find report id for daycode=%s, listcode=%s", 
				daycode, onepath.getName());
			
			int delold = UserIdxDb.deleteOld(reportid, "lift_report");
			if(delold > 0)
				{ Util.pf("Deleted %d old entries for reportid %d\n", delold, reportid); }
			
			List<String> liftlines = HadoopUtil.readFileLinesE(fsystem, onepath);
			
			for(String onelift : liftlines)
			{
				String[] rank_score = onelift.split("\t");
				String towrite = Util.sprintf("%d\t%s\n", reportid, onelift);
				bwrite.write(towrite);
			}
		}
		
		bwrite.close();
		int numup = UserIdxDb.loadLiftReport();
		_logMail.pf("Uploaded %d rows into database.\n", numup);
	}	
	
	
	static Map<String, Integer> readListPartMap(JobConf job) throws IOException
	{
		String blockend = job.get(TARG_DAY_CODE);
		FileSystem fsys = FileSystem.get(job);
		
		// Just read first couple of lines of the first shard file
		String oneshard = Util.sprintf("/userindex/userscores/%s/shard_00006/part-00000", blockend);
		BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, oneshard);
		Map<String, Integer> partmap = Util.treemap();
		
		int lcount = 0;
		for(String logline = bread.readLine(); logline != null; logline = bread.readLine())
		{
			String listcode = logline.split("\t")[1];
			
			if(!partmap.containsKey(listcode))
				{  partmap.put(listcode, partmap.size());	}
			
			if(++lcount > 10000)
				{ break; }
		}
		
		Util.massert(lcount > 10000, "Read only %d lines from shard file %s, perhaps there is a problem with shard", lcount, oneshard);
		
		return partmap;
	}
	
	public static class DumbScoreMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		Map<String, Integer> listPartMap;
		
		public void configure(JobConf jobconf)
		{
			try { listPartMap = readListPartMap(jobconf); }
			catch (IOException ioex) { throw new RuntimeException(ioex); }	
		}		
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			String[] toks = value.toString().split("\t");	
			
			// This will break if the input strings are not formatted in the correct
			// wtp__listcode__score__ctrycode format, but that's the right behavior
			String id = toks[0];
			String listcode = toks[1];
			double score = Double.valueOf(toks[2]);
			String ctrycode = toks[3].trim();
			
			// Skip if the target code for the list is not the same as the user's country
			String targcode = ListInfoManager.getSing().getCountryTargForList(listcode).toString();
			if(!ctrycode.equals(targcode))
				{ return; }
			
			// Okay, so now cookies with a HIGH score are going to be sorted to the TOP of the file.
			double modscore = 5000-score;
			
			if(!listPartMap.containsKey(listcode))
				{ throw new RuntimeException("Found listcode not in partmap: " + listcode);} 
			
			String combkey = Util.sprintf("%s%s%.05f", listcode, Util.DUMB_SEP, modscore);
			output.collect(new Text(combkey), new Text(score+"\t"+id+"\t"+ctrycode));
		}
	}	
	
	// Just take the top N elements.
	public static class HeadReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		int _outputCount = 0;
		
		// Each reducer gets one and only one positive request. Is this the right way to do things...?
		PosRequest _posReq;
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			String[] listcode_score = key.toString().split(Util.DUMB_SEP);
			
			if(_posReq == null)
			{
				Util.massert(ListInfoManager.getSing().havePosRequest(listcode_score[0]),
					"No positive request found for listcode %s", listcode_score[0]);
				
				_posReq = ListInfoManager.getSing().getPosRequest(listcode_score[0]);
			}

			Util.massert(_posReq.getListCode().equals(listcode_score[0]),
				"Mixup on reducer side, have posreq listcode %s and record listcode %s",
				_posReq.getListCode(), listcode_score[0]);
						
			while(_outputCount < (_posReq.getTargSizeK() * 1000) && values.hasNext())
			{
				collector.collect(key, values.next());
				_outputCount++;
			}
		}		
	}		
	
	public static class ChartReducer extends MapReduceBase implements org.apache.hadoop.mapred.Reducer<Text, Text, Text, Text>
	{
		int userCount = 0;
		String prevScore = null;
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			while(values.hasNext())
			{
				userCount++;
				
				String[] score_id_ctry = values.next().toString().split("\t");
				double curscore = Double.valueOf(score_id_ctry[0]);
				String truncscore = Util.sprintf("%.02f", curscore);
				
				if(!truncscore.equals(prevScore))
				{
					// Output BOTH the previous and the current user, with the same score
					collector.collect(new Text((userCount-1)+""), new Text(truncscore));
					collector.collect(new Text(userCount+""), new Text(truncscore));
					prevScore = truncscore;
				}
			}
		}		
	}	

	
	public static class ListCodePartitioner implements Partitioner<Text, Text>
	{	
		Map<String, Integer> listPartMap;
		
		public void configure(JobConf jobconf)
		{
			try { listPartMap = readListPartMap(jobconf); }
			catch (IOException ioex) { throw new RuntimeException(ioex); }	
		}		
		
		public int getPartition(Text key, Text value, int numPart)
		{
			// maybe running in LocalMode?
			if(numPart == 1) { return 0; }
			
			String listcode = key.toString().split(Util.DUMB_SEP)[0];
			
			if(!listPartMap.containsKey(listcode))
				{ throw new RuntimeException("Found listcode not in partmap: " + listcode);} 
			
			if(listPartMap.get(listcode) >= numPart)
			{
				String emssg = Util.sprintf("Listcode=%s --> %d, partmap = %s",
					listcode, listPartMap.get(listcode), listPartMap);
				
				throw new RuntimeException(emssg);
				
			}
			
			return listPartMap.get(listcode);
		}
	}		
}
