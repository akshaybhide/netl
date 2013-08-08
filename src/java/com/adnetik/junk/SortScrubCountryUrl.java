
package com.adnetik.userindex;

import java.io.IOException;
import java.util.*;

//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.ArrayWritable;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.userindex.UserIndexUtil;


public class SortScrubCountryUrl extends Configured implements Tool
{	
	private static List<Character> _CHAR_LIST = Util.vector();

	static { initCharList(); }	
	
	public enum ViewCounter { VIEW0, VIEW1, VIEW2, VIEW3, VIEW4, VIEW5, VIEW6, VIEW7, VIEW8, VIEW9 };
	


	public static final Random JRAND = new Random();
	
	public static final String BASE_PATH = "/userindex/sortscrub";
	
	public static final String MAGIC_SEPARATOR = "IxxxIxxxI";
	
	public static final String USER_COUNT_PREF = "countpref";
	
	private SimpleMail logMail = new SimpleMail("SSCU_report");
	
	public static void main(String[] args) throws Exception
	{
		int subCode = ToolRunner.run(new SortScrubCountryUrl(), args);
		System.exit(subCode);
	}
	
	public int run(String[] args) throws Exception
	{	
		getConf().setInt("mapred.job.reuse.jvm.num.tasks", 10);				
		// HadoopUtil.setLzoOutput(this);
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());
		
		if(args.length == 0 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: SortScrub <daycode>\n");	
			return 1;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);

		Map<String, String> optargs = Util.getClArgMap(args);
		Integer maxfile = (optargs.containsKey("maxfile") ? Integer.valueOf(optargs.get("maxfile")) : Integer.MAX_VALUE);

		// DO this in HdfsCleanup
		// deleteOldData(UserIndexUtil.DATA_SAVE_WINDOW, fSystem); 
			
		// align
		{
			Text a = new Text("");			
			LongWritable lw = new LongWritable(0);
			HadoopUtil.alignJobConf(job, new CombinedMapper(), new CombinedReducer(), a, a, a, a);
		}
		
		// Just want to sort the no_bid data by wtp_user_id
		{
			List<Path> pathlist = Util.vector();
			List<String> patterns = Util.vector(); 
			
			patterns.add(Util.sprintf("/data/no_bid/%s/*/*.log.gz", daycode));
			patterns.add(Util.sprintf("/data/bid_all/%s/*/*.log.gz", daycode));
			//patterns.add(Util.sprintf("/data/bid_all/%s/casale/*.log.gz", daycode));
			
			for(String onepatt : patterns)
			{ 
				for(Path mpath : HadoopUtil.getGlobPathList(fSystem, onepatt))
				{
					if(pathlist.size() < maxfile)
						{ pathlist.add(mpath); }
				}
			}
									
			messagePf("\nFound %d input files", pathlist.size());
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));	
		}
		
		{
			Path outputPath = new Path("/userindex/catchup/sortscrub/" + daycode);
			fSystem.delete(outputPath, true);
			messagePf("\nUsing output dir %s", outputPath);
			FileOutputFormat.setOutputPath(job, outputPath);			
		}
		
		
		// Specify various job-specific parameters     
		job.setJobName(getJobName(daycode));
		job.setNumReduceTasks(HadoopUtil.MAX_REDUCER_NODES-1); // leave one reducer on each node
		job.setPartitionerClass(CombinedPartitioner.class);
		
		messagePf("\nSubmitting SortScrubCountryUrl for %s", daycode);
		// Submit the job, then poll for progress until the job is complete
		RunningJob jobrun = JobClient.runJob(job);		
		HadoopUtil.checkSendFailMail(jobrun, this);
		
		messagePf("\nFinished SortScrubCountryUrl");	
		logMail.send2admin();
		
		return 0;
	}
	
	public static String getJobName(String daycode)
	{
		return Util.sprintf("SortScrubCountryUrl %s", daycode);
	}
	
	private void messagePf(String s, Object... args)
	{
		String m = Util.sprintf(s, args);
		Util.pf(m);
		logMail.addLogLine(m.trim());	// Get rid of newlines	
	}
	
	public static String randomUserId()
	{
		StringBuilder sb = new StringBuilder();

		while(sb.length() < 12)
		{
			sb.append(_CHAR_LIST.get(JRAND.nextInt(_CHAR_LIST.size())));
		}
		
		return sb.toString();
	}	
	
	
	private static void initCharList()
	{
		for(int i = 48; i <= 57; i++)
		{
			_CHAR_LIST.add((char) i);
		}
		
		for(int i = 65; i <= 90; i++)
		{
			_CHAR_LIST.add((char) i);
		}
	}
	
	
	public static class CombinedMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		public static Set<String> _CTY_WHITELIST = null;

		Random jRand = new Random();
		public static final double DISCARD_PROB = .66;
		
		public static final double USER_POOL_DISCARD_PROB = .5;
		
		LogVersion fileVers; 
		LogVersion targVers; 
				
		boolean loggedMismatch = false;
		
		static Set<String> getCtyWhiteSet()
		{
			if(_CTY_WHITELIST == null)
			{
				_CTY_WHITELIST = Util.treeset();
				_CTY_WHITELIST.add("US");
				_CTY_WHITELIST.add("GB");
				_CTY_WHITELIST.add("NL");
				_CTY_WHITELIST.add("ES");
				_CTY_WHITELIST.add("BR");
				_CTY_WHITELIST.add("MX");
				_CTY_WHITELIST.add("DE");
				_CTY_WHITELIST.add("FR");
				_CTY_WHITELIST.add("CA");
			}
			
			return _CTY_WHITELIST;			
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{
			String line = value.toString();
			
			// The file version is just the format of the log file we're running against....
			if(fileVers == null)
			{
				fileVers = HadoopUtil.fileVersionFromReporter(reporter);
			}
					
			try {
				// Okay, open the BLE using the fileVersion, but transform it if necessary.
				// Transformation is necessary because of PhaseOne output includes log lines.
				BidLogEntry ble = new BidLogEntry(LogType.no_bid_all, fileVers, line);

				phaseOne(ble, output);
				phaseTwo(ble, output);
				phaseThree(ble, output);
				
			} catch (BidLogFormatException blex) {
				
				reporter.incrCounter(blex.e, 1);
			}
		}
		
		
		private void phaseOne(BidLogEntry ble, OutputCollector<Text, Text> output) throws IOException
		{
			String wtpid = ble.getField(LogField.wtp_user_id).trim();
			
			Util.massert(false, "Need to fix the WTP Cutoff code before using this code");
			
			// Can't do anything with non-set WTP ids
			/*
			if(wtpid.length() == WtpId.VALID_WTP_LENGTH && wtpid.compareTo(UserIndexUtil.WTP_CUTOFF) < 0)
			{ 
				// Transform to minimal version to cut down on data transfer size
				BidLogEntry minimal = ble.transformToVersion(LogType.UIndexMinType, LogVersion.UIndexMinVers2);
				output.collect(new Text(wtpid), new Text(minimal.getLogLine()));
			}			
			*/
		}
		
		// Country - URL output for AIM Page Index
		private void phaseTwo(BidLogEntry ble, OutputCollector<Text, Text> output) throws IOException
		{
			if(jRand.nextDouble() < DISCARD_PROB)
				{ return; }
			
			String country = ble.getField(LogField.user_country).trim();
			
			if(country.length() == 0)
				{ return; }
			
			if(!getCtyWhiteSet().contains(country))
				{ return; }
			
			String url = ble.getField(LogField.url).trim();
			
			if(url.length() < 4)
				{ return; }
			
			if(!url.startsWith("http"))
				{ return; }
			
			String id = ble.getField(LogField.wtp_user_id).trim();
			id = (id.length() < 3 ? ble.getField(LogField.exchange_user_id) : id);
			id = (id.length() < 3 ? randomUserId() : id );
			
			String vrt = ble.getField(LogField.google_main_vertical).trim();
			vrt = (vrt.length() == 0 ? "NONE" : vrt);
			
			StringBuilder outkey = new StringBuilder();
			{
				outkey.append(url);
				outkey.append(MAGIC_SEPARATOR);
				outkey.append(country);
				outkey.append(MAGIC_SEPARATOR);
				outkey.append(vrt);
				outkey.append(MAGIC_SEPARATOR);
				outkey.append(id);
			}
					
			output.collect(new Text(outkey.toString()), HadoopUtil.TEXT_ONE);
		}	

		// User count pref		
		private void phaseThree(BidLogEntry ble, OutputCollector<Text, Text> output) throws IOException
		{	
			if(jRand.nextDouble() < USER_POOL_DISCARD_PROB)
				{ return; }
			
			String id = ble.getField(LogField.wtp_user_id).trim();
			String cty = ble.getField(LogField.user_country).trim();
			
			if(id.length() > 0)
			{
				output.collect(new Text(USER_COUNT_PREF + Util.DUMB_SEP + id + Util.DUMB_SEP + cty), HadoopUtil.TEXT_ONE);
			}
		}			
	}	
	
	public static class CombinedReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{
		String curUrl = null;
		String curCty = null;
		String curVrt = null;
		int hitCount = 0;
		
		Set<Character> viewPrefs = Util.treeset();
		
		public void reduce(Text key , Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) 
		throws IOException
		{
			String keystr = key.toString();
			
			if(keystr.startsWith(USER_COUNT_PREF)) {
			
				// key is of form <pref>____<wtpid>____<cty>
				String[] pref_wtp_cty = keystr.split(Util.DUMB_SEP);
				
				if(pref_wtp_cty.length < 3)
					{ return; }
				
				int hcount = 0;
				
				while(values.hasNext())
				{
					values.next();
					hcount++; 
				}		
				
				collector.collect(new Text(pref_wtp_cty[1]), new Text(pref_wtp_cty[2] + "\t" + hcount));
			
			} else if(keystr.indexOf(MAGIC_SEPARATOR) > -1) {
				
				// URL-country reducer
				String[] url_cty_vert_id = keystr.split(MAGIC_SEPARATOR);
				
				if(!url_cty_vert_id[0].equals(curUrl) || !url_cty_vert_id[1].equals(curCty) || !url_cty_vert_id[2].equals(curVrt))
				{
					if(curUrl != null)
					{
						String outkey = Util.sprintf("%s\t%s\t%s", url_cty_vert_id[0], url_cty_vert_id[1], url_cty_vert_id[2]);
						collector.collect(new Text(outkey), new Text("" + hitCount));
					}
					
					curUrl = url_cty_vert_id[0];
					curCty = url_cty_vert_id[1];
					curVrt = url_cty_vert_id[2];
					hitCount = 0;
				}
				
				while(values.hasNext())
				{
					values.next();
					hitCount++; 
				}
				
			} else if(keystr.length() > 0) {
				
				// Report if we are seeing a new prefix
				{
					char cpref = keystr.charAt(0);
					
					if(!viewPrefs.contains(cpref))
					{
						viewPrefs.add(cpref);
						
						try { 
							ViewCounter vc = ViewCounter.valueOf("VIEW" + cpref);
							reporter.incrCounter(vc, 1);
						} catch (Exception ex) { }
					}
				}
				
				// Empty reducer
				while(values.hasNext())
				{
					collector.collect(key, values.next());
				}
			}
		}	
	}		
	
	public static class CombinedPartitioner implements Partitioner<Text, Text>
	{
		public void configure(JobConf jobconf) {
			//super.configure(jobconf);
		}
		
		public int getPartition(Text key, Text value, int numPart)
		{
			String s = key.toString();
			
			if(s.startsWith(USER_COUNT_PREF)) {
				return 28;				
			}
				
			if(s.indexOf(MAGIC_SEPARATOR) > -1)
			{
				String[] url_cty_vrt_id = s.split(MAGIC_SEPARATOR);
				long hc = url_cty_vrt_id[0].hashCode() + url_cty_vrt_id[1].hashCode() + url_cty_vrt_id[2].hashCode();
				hc = (hc < 0 ? -hc : hc); // Otherwise we'll generate negative partition data			
				hc = (hc % 4);
				
				// This is a Country-Url output pair
				return ((int) hc) + 24;
			} 
			
			// Here, S is a wtp Id
			return UserIndexUtil.uniformWtpPartition(s);  
		}
	}	
}
