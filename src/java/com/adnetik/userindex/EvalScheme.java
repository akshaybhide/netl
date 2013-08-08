
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


public class EvalScheme extends Configured implements Tool
{			
	
	public enum DataTypeCounter { shard, pixel }
	
	private enum OutputType { userscore, pixelfire };
	
	SimpleMail _logMail;
	
	public static void main(String[] args) throws Exception 
	{
		Map<String, String> optargs = Util.getClArgMap(args);
		
		// Crontab will run using dow=thu maybe
		if(optargs.containsKey("dow"))
		{
			ShortDowCode targdow = ShortDowCode.valueOf(optargs.get("dow"));
			ShortDowCode currdow = TimeUtil.getDowCode(TimeUtil.getTodayCode());
			
			if(targdow != currdow)
			{ 
				Util.pf("DOW target is %s, today is %s\n", targdow, currdow);	
				return;
			}
		}		
		
		int mainCode = HadoopUtil.runEnclosingClass(args);
		Util.pf("\n");
		
		// hdfs2db("2012-06-24");
	} 
	
	public int run(String[] args) throws Exception
	{
		String curcanday = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
		Map<String, String> optargs = Util.getClArgMap(args);
		curcanday = optargs.containsKey("curcanday") ? optargs.get("curcanday") : curcanday;
		
		_logMail = new SimpleMail("AIdxWeekly EvalSchemeReport for CurCanDay="+curcanday);		
		int exitcode = runsub(args, curcanday);
		_logMail.send2admin();
		return exitcode;
	}
	
	public int runsub(String[] args, String curcanday) throws Exception
	{
		// Going to be using mixed LZO+non-LZO
		// getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fsys = FileSystem.get(getConf());
		
		TreeSet<String> pixdatadays = Util.treeset();
		for(String pixday = curcanday; pixdatadays.size() < 14; pixday = TimeUtil.dayBefore(pixday))
			{ pixdatadays.add(pixday); }
		
		String evalday = TimeUtil.dayBefore(pixdatadays.first());
		
		_logMail.pf("curcanday is %s, evalday is %s, pixdata is \n%s\n", curcanday, evalday, pixdatadays);
		
		ArgMap optmap = Util.getClArgMap(args);
		boolean dohadoop = optmap.getBoolean("dohadoop", true);
		boolean doupload = optmap.getBoolean("doupload", true);	
		
		// This is the number of score files we require to find before starting the job
		// Sometimes one of the files breaks, so we can override here to run job by hand
		// Default is number of shuffle partitions times 15
		int numscorefile = optmap.getInt("numscorefile", UserIndexUtil.NUM_SHUF_PARTITIONS*15);
		
		// This is going to be true by default until we fix the LZO stuff
		boolean usenfs = optmap.getBoolean("usenfs", true);
				
		if(dohadoop)
		{
			// align
			{
				Text a = new Text("");			
				LongWritable lw = new LongWritable(0);
				HadoopUtil.alignJobConf(job, true, new EvalMapper(), new EvalReducer(), null,
					a, a, a, a);
			}
			
			// Inputs are : 14 days of pixel data, 2-week previous canday's user scores.
			{
				List<Path> pathlist = Util.vector();
				for(String oneday : pixdatadays)
				{
					if(usenfs) { 
						
						List<String> nfspixlist = Util.getNfsPixelLogPaths(oneday);
						nfspixlist = (nfspixlist == null ? new Vector<String>() : nfspixlist);
						
						for(String pixpath : nfspixlist)
						{
							Path p = new Path("file://" + pixpath);
							pathlist.add(p);
						}
						
						_logMail.pf("Using NFS, found %d pixel paths for daycode %s\n", nfspixlist.size(), oneday);
					
					} else {
						
						Path p = new Path(Util.sprintf("/data/pixel/pix_%s.lzo", oneday));
						pathlist.add(p);
						if(!fsys.exists(p))
						{ 
							_logMail.pf("Error: could not find pixel LZO file %s\n", p);
							return 1;
						}
					}					
				}
				
				{
					String scorepatt = Util.sprintf("/userindex/userscores/%s/shard*/part-*", evalday);
					List<Path> scorelist = HadoopUtil.getGlobPathList(fsys, scorepatt);
					_logMail.pf("Found %d user score paths", scorelist.size());
					Util.massert(scorelist.size() == numscorefile,
						"Scorelist size is %d, expected %d", scorelist.size(), numscorefile);

					pathlist.addAll(scorelist);
				}
				
				FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
			}
			
			// job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
			job.setNumReduceTasks(15); 	 	
			
			Path outputPath = new Path(Util.sprintf("/userindex/evaluation/%s/methA", evalday));
			HadoopUtil.moveToTrash(fsys, outputPath);
			// fsys.delete(outputPath, true);
			FileOutputFormat.setOutputPath(job, outputPath);			
			
			// Specify various job-specific parameters     
			job.setJobName(Util.sprintf("Evaluation Method A %s", evalday));
			
			RunningJob jobrun = JobClient.runJob(job);	
		}
		
		if(doupload)
		{
			// Load the resulting data into the DB.
			hdfs2db(evalday);
		}
		
		return 0;
	}
	
	private void hdfs2db(String daycode) throws IOException
	{
		// compile all the paths into a TSV, then do batch upload
		FileSystem fsystem = FileSystem.get(new Configuration());
		
		Map<String, SortedSet<Double>> scoremap = Util.treemap();
		{
			String liftpatt = Util.sprintf("/userindex/evaluation/%s/methA/part*", daycode);
			List<Path> pathlist = HadoopUtil.getGlobPathList(fsystem, liftpatt);
			
			for(Path onepath : pathlist)
			{
				List<String> evallines = HadoopUtil.readFileLinesE(fsystem, onepath);
				_logMail.pf("Read %d data lines from file %s\n", evallines.size(), onepath);
				
				for(String oneline : evallines)
				{
					String[] lcode_score = oneline.split("\t");
					String listcode = lcode_score[0];
					Double score = Double.valueOf(lcode_score[1]);
					
					Util.setdefault(scoremap, listcode, new TreeSet<Double>());
					scoremap.get(listcode).add(score);
				}
			}
		}
		
		_logMail.pf("Found %d listcodes with scores\n", scoremap.size());
		
		Map<String, Integer> repmap = Util.treemap();
		BufferedWriter bwrite = FileUtils.getWriter(UserIdxDb.EVAL_DUMP_FILE);
		int writecount = 0;
		
		for(String listcode : scoremap.keySet())
		{
			PosRequest posreq = ListInfoManager.getSing().getPosRequest(listcode);
			
			if(!repmap.containsKey(listcode))
			{
				Integer repid = UserIdxDb.lookupCreateRepId(daycode, posreq);
				
				int delold = UserIdxDb.deleteOld(repid, "eval_scheme");
				if(delold > 0)
					{ _logMail.pf("Deleted %d old entries for reportid %d\n", delold, repid); }
				
				repmap.put(listcode, repid);
			}
			
			Integer reportid = repmap.get(listcode);
			List<Double> scorelist = new Vector<Double>(scoremap.get(listcode));
			Collections.reverse(scorelist);
			
			for(int i = 0; i < scorelist.size(); i++)
			{
				// Writing reportid, user_rank, user_score
				String towrite = Util.sprintf("%d\t%d\t%s\n", reportid, i, scorelist.get(i));
				bwrite.write(towrite);
			}
			
			writecount++;
			_logMail.pf("Wrote %d lines for listcode %s, reportid=%d, %d complete out of %d\n",
				scorelist.size(), listcode, reportid, writecount, scoremap.size());
		}
		
		bwrite.close();
		int numup = UserIdxDb.loadEvalData();
		_logMail.pf("Uploaded %d rows into database.\n", numup);		
	}
	
	public static class EvalMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		Boolean isPixelData = null;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			if(isPixelData == null)
			{
				String path = reporter.getInputSplit().toString();
				isPixelData = path.indexOf("pixel") > -1;
				reporter.incrCounter(HadoopUtil.Counter.PathChecks, 1); 
				reporter.incrCounter((isPixelData ? DataTypeCounter.pixel : DataTypeCounter.shard), 1);
			}
			
			if(isPixelData)
			{
				PixelLogEntry ple = new PixelLogEntry(value.toString());
				String wid = ple.getField(LogField.wtp_user_id);
				if(wid.trim().length() != WtpId.VALID_WTP_LENGTH)
					{ return; }
				
				int pixid = ple.getIntField(LogField.pixel_id);
				String combval = Util.sprintf("%s\t%d", OutputType.pixelfire, pixid);
				
				output.collect(new Text(wid), new Text(combval));
				
			} else {
				
				// id/listcode/score/country
				String[] toks = value.toString().split("\t");		
				String combval = Util.sprintf("%s\t%s\t%s", OutputType.userscore, toks[1], toks[2]);
				output.collect(new Text(toks[0]), new Text(combval));
			}
		}
	}		
	
	public static class EvalReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text,Text> collector, Reporter reporter) 
		throws IOException
		{
			
			Set<Integer> pixelfire = Util.treeset();
			SortedMap<String, Double> list2score = Util.treemap();			
			
			while(values.hasNext())
			{
				String[] toks = values.next().toString().split("\t");
				OutputType otype = OutputType.valueOf(toks[0]);
				
				if(otype == OutputType.pixelfire)
				{
					Integer pixid = Integer.valueOf(toks[1]);
					pixelfire.add(pixid);
					
				} else {
					// otype == OutputType.userscore
					String listcode = toks[1];
					Double score = Double.valueOf(toks[2]);
					list2score.put(listcode, score);
				}
			}
			
			for(Integer onepix : pixelfire)
			{
				String targlist = Util.sprintf("pixel_%d", onepix);
				
				// greater than or equal to targlist
				SortedMap<String, Double> tmap = list2score.tailMap(targlist);
				
				// Want to do an output for all the listcodes that start with targlist code
				for(String listkey : tmap.keySet())
				{
					if(!listkey.startsWith(targlist))
						{ break; }
					
					collector.collect(new Text(listkey), new Text(""+list2score.get(listkey))); 
				}
			}
		}		
	}		
	
}
